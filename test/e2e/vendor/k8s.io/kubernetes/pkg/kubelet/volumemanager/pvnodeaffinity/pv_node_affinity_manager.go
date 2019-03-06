/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pvnodeaffinity

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kubetypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/pod"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	orphan_quotapath_cleanperiod time.Duration = 1 * time.Minute
)

type PodInfo struct {
	Info string
}
type MountInfo struct {
	HostPath             string
	VolumeQuotaSize      int64
	VolumeCurrentSize    int64
	VolumeCurrentFileNum int64
	PodInfo              *PodInfo
}
type MountInfoList []MountInfo

func (mifl MountInfoList) Len() int { return len(mifl) }
func (mifl MountInfoList) Less(i, j int) bool {
	return mifl[i].HostPath < mifl[j].HostPath
}
func (mifl MountInfoList) Swap(i, j int) {
	mifl[i], mifl[j] = mifl[j], mifl[i]
}

type HostPathPVMountInfo struct {
	NodeName   string
	MountInfos MountInfoList
}

type HostPathPVMountInfoList []HostPathPVMountInfo

func (hppmil HostPathPVMountInfoList) Len() int { return len(hppmil) }
func (hppmil HostPathPVMountInfoList) Less(i, j int) bool {
	return hppmil[i].NodeName < hppmil[j].NodeName
}
func (hppmil HostPathPVMountInfoList) Swap(i, j int) {
	hppmil[i], hppmil[j] = hppmil[j], hppmil[i]
}

type PVNodeAffinity interface {
	Run(stopCh <-chan struct{})
	Add(item PVSyncItem)
	Remove(item PVSyncItem)
}

type mountedPods struct {
	mountedPods map[volumetypes.UniquePodName]sets.String
	lock        sync.RWMutex
}

func (mp *mountedPods) isPodMounted(podName volumetypes.UniquePodName) bool {
	mp.lock.RLock()
	defer mp.lock.RUnlock()
	pvnames, _ := mp.mountedPods[podName]
	return len(pvnames) > 0
}
func (mp *mountedPods) getPodMountedPvNames(podName volumetypes.UniquePodName) []string {
	mp.lock.RLock()
	defer mp.lock.RUnlock()
	names, _ := mp.mountedPods[podName]
	return names.UnsortedList()
}
func (mp *mountedPods) addPodMountedPV(podName volumetypes.UniquePodName, pvname string) error {
	mp.lock.Lock()
	defer mp.lock.Unlock()
	names, exists := mp.mountedPods[podName]
	if exists == false {
		names = sets.NewString(pvname)
		mp.mountedPods[podName] = names
		return nil
	} else {
		if names.Has(pvname) {
			return fmt.Errorf("addPodMounted pod:%s has mounted to pv:%s", podName, pvname)
		}
		names.Insert(pvname)
		mp.mountedPods[podName] = names
		return nil
	}
}

func (mp *mountedPods) removePodMountedPV(podName volumetypes.UniquePodName, pvname string) error {
	mp.lock.Lock()
	defer mp.lock.Unlock()
	names, exists := mp.mountedPods[podName]
	if exists == false {
		return fmt.Errorf("removePodMounted pod:%s has not any mounted pv", podName)
	} else {
		if names.Has(pvname) == false {
			return fmt.Errorf("removePodMounted pod:%s has not mounted to pv:%s", podName, pvname)
		}
		names.Delete(pvname)
		if len(names) == 0 {
			delete(mp.mountedPods, podName)
		} else {
			mp.mountedPods[podName] = names
		}
		return nil
	}
}
func (mp *mountedPods) removePodAllMountedPV(podName volumetypes.UniquePodName) {
	mp.lock.Lock()
	defer mp.lock.Unlock()
	delete(mp.mountedPods, podName)
}

type ReadyFun func() bool

type pvNodeAffinity struct {
	kubeClient      clientset.Interface
	updateDuration  time.Duration
	nodeName        string
	xfsQuotaManager xfs.DiskQuotaManager
	nextHandle      chan PVSyncItem
	needsyncpv      map[string]PVSyncItem
	mx              sync.Mutex
	pvLister        corelisters.PersistentVolumeLister
	podManager      pod.Manager
	isPvSynced      bool
	isReady         ReadyFun
}

type PVSyncItem struct {
	PVName    string
	MountPath string
	PodUid    string
	PodNS     string
	PodName   string
}

func NewPVNodeAffinity(kubeClient clientset.Interface,
	updateDuration time.Duration,
	nodeName string,
	xfsQuotaManager xfs.DiskQuotaManager,
	podManager pod.Manager,
	pvLister corelisters.PersistentVolumeLister,
	isReady ReadyFun) PVNodeAffinity {
	return &pvNodeAffinity{
		kubeClient:      kubeClient,
		updateDuration:  updateDuration,
		nodeName:        nodeName,
		xfsQuotaManager: xfsQuotaManager,
		nextHandle:      make(chan PVSyncItem, 500),
		needsyncpv:      make(map[string]PVSyncItem),
		mx:              sync.Mutex{},
		pvLister:        pvLister,
		podManager:      podManager,
		isPvSynced:      false,
		isReady:         isReady,
	}
}
func (pna *pvNodeAffinity) Run(stopCh <-chan struct{}) {
	go wait.Until(pna.doClearOrphanQuotaPath, orphan_quotapath_cleanperiod, stopCh)
	go wait.Until(pna.doWork, time.Second, stopCh)
	wait.Until(pna.populatorLoopFunc(), pna.updateDuration, stopCh)
}
func (pna *pvNodeAffinity) doClearOrphanQuotaPath() {
	err := pna.clearOrphanQuotaPath()
	if err != nil {
		glog.Errorf("doClearOrphanQuotaPath err:%v", err)
	}
}

func (pna *pvNodeAffinity) clearOrphanQuotaPath() error {
	if pna.xfsQuotaManager == nil || pna.pvLister == nil {
		return nil
	}
	pvs, err := pna.pvLister.List(labels.Everything())
	if err != nil {
		return err
	}
	if pna.isPvSynced == false && len(pvs) == 0 {
		pvsList, errList := pna.kubeClient.Core().PersistentVolumes().List(metav1.ListOptions{})
		if errList != nil {
			return errList
		}
		pvs = make([]*v1.PersistentVolume, 0, len(pvsList.Items))
		for i, _ := range pvsList.Items {
			pvs = append(pvs, &pvsList.Items[i])
		}
	}
	pna.isPvSynced = true
	activeQuotaPath := make(map[string]bool)
	for _, pv := range pvs {
		if pv.Spec.HostPath == nil && pv.Spec.CSI == nil { // skip not hostpath pv
			continue
		}
		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			continue
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]
		hppmil := HostPathPVMountInfoList{}
		err := json.Unmarshal([]byte(mountInfo), &hppmil)
		if err != nil {
			continue
		}
		isRetain := true
		if pv.Status.Phase == v1.VolumeAvailable && pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimRecycle {
			isRetain = false
		}
		for _, nodeInfo := range hppmil {
			if nodeInfo.NodeName == pna.nodeName {
				for _, pathInfo := range nodeInfo.MountInfos {
					if isKeepPV(pv) == false {
						var podUid, podNs, podName string
						if pathInfo.PodInfo != nil {
							strs := strings.Split(pathInfo.PodInfo.Info, ":")
							if len(strs) == 3 {
								podNs, podName, podUid = strs[0], strs[1], strs[2]
							}
						}
						pna.nextHandle <- PVSyncItem{
							PVName:    pv.Name,
							MountPath: pathInfo.HostPath,
							PodUid:    podUid,
							PodNS:     podNs,
							PodName:   podName,
						}
					}
					activeQuotaPath[getStandardPathString(pathInfo.HostPath)] = isRetain
				}
				break
			}
		}
	}
	activeFun := func(podid, pvid string) bool {
		if podid == "" && pvid != "" {
			for _, pv := range pvs {
				if pv.Spec.HostPath == nil { // skip not hostpath pv
					continue
				}
				if string(pv.UID) == pvid {
					return true
				}
			}
			return false
		}
		if podid == "" {
			return false
		}
		_, ok := pna.podManager.GetPodByUID(kubetypes.UID(podid))
		return ok
	}
	return pna.xfsQuotaManager.CleanOrphanQuotaPath(activeQuotaPath, activeFun)
}
func (pna *pvNodeAffinity) Add(item PVSyncItem) {
	pna.nextHandle <- item
	pna.mx.Lock()
	defer pna.mx.Unlock()
	pna.needsyncpv[getStandardPathString(item.MountPath)] = item
}

func (pna *pvNodeAffinity) Remove(item PVSyncItem) {
	pna.nextHandle <- item
	pna.mx.Lock()
	defer pna.mx.Unlock()

	if oldItem, ok := pna.needsyncpv[getStandardPathString(item.MountPath)]; ok {
		if oldItem.PodUid != item.PodUid {
			return
		}
	}
	delete(pna.needsyncpv, getStandardPathString(item.MountPath))
}

func (pna *pvNodeAffinity) doWork() {
	for {
		select {
		case item := <-pna.nextHandle:
			pod, ok := pna.podManager.GetPodByUID(kubetypes.UID(item.PodUid))
			if pod != nil && ok {
				if pod.Status.Phase == v1.PodRunning {
					// we record to pv annotation after pod is running
					pna.sync(item.PodUid, item.MountPath, item.PVName, item.PodNS, item.PodName)
				} else {
					glog.Infof("pvNodeAffinity skip sync pv:%s for pod:%s:%s because pod is %s\n",
						item.PVName, pod.Namespace, pod.Name, pod.Status.Phase)
				}
			} else {
				pna.sync(item.PodUid, item.MountPath, item.PVName, item.PodNS, item.PodName)
			}
		}
	}
}

func (pna *pvNodeAffinity) populatorLoopFunc() func() {
	return func() {
		for _, item := range pna.needsyncpv {
			pna.nextHandle <- item
		}
	}
}

func (pna *pvNodeAffinity) calcPVNodeInfo(historyInfo, nodename, pvuid, mountPath, podInfo string, keepPV bool) (string, error) {
	hppmil := HostPathPVMountInfoList{}
	if historyInfo != "" {
		err := json.Unmarshal([]byte(historyInfo), &hppmil)
		if err != nil {
			return "", fmt.Errorf("calcPVNodeInfo Unmarshal[%s] error:%v", historyInfo, err)
		}
	}
	mil := getPVQuotaInfo(pvuid, pna.xfsQuotaManager, mountPath, podInfo)

	newhppmil := HostPathPVMountInfoList{}
	for _, info := range hppmil {
		if info.NodeName != "" {
			newhppmil = append(newhppmil, info)
		}
	}
	for i, _ := range newhppmil {
		if newhppmil[i].NodeName == nodename {
			if keepPV {
				newhppmil[i].MountInfos = mergeMountInfoList(newhppmil[i].MountInfos, mil,
					func(path string) bool {
						return true
					})
			} else {
				newhppmil[i].MountInfos = mergeMountInfoList(newhppmil[i].MountInfos, mil,
					func(path string) bool {
						ok, info := pna.xfsQuotaManager.IsPathXFSQuota(path)
						if ok == false {
							return false
						}
						if info.SubId != "" && pna.isReady() {
							_, ok = pna.podManager.GetPodByUID(kubetypes.UID(info.SubId))
							if ok == false {
								fmt.Printf("remove path because pod %s is not exit, %v\n", info.PodId, info)
							}
						}
						return ok
					})
			}
			if len(newhppmil[i].MountInfos) == 0 {
				newhppmil = append(newhppmil[:i], newhppmil[i+1:]...)
			}
			bytes, err := json.Marshal(newhppmil)
			if err != nil {
				return "", err
			}
			return string(bytes), nil
		}
	}
	//curTime := time.Now()
	newInfo := HostPathPVMountInfo{
		NodeName:   nodename,
		MountInfos: mil,
	}
	if len(mil) > 0 {
		newhppmil = append(newhppmil, newInfo)
	}
	sort.Sort(newhppmil)
	bytes, err := json.Marshal(newhppmil)
	if err != nil {
		return "", fmt.Errorf("calcPVNodeInfo Marshal error:%v", err)
	}
	return string(bytes), nil
}
func isNeedAddPodInfo(pv *v1.PersistentVolume) bool {
	// only keep and true pv add pod info now. The pod info is used for node time out delete pod
	if pv == nil || pv.Annotations == nil ||
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] != "true" {
		return false
	}
	return true
}

func isHostpathPVKeep(pv *v1.PersistentVolume) bool {
	if pv == nil || pv.Annotations == nil ||
		pv.Annotations[xfs.PVHostPathMountPolicyAnn] != "none" {
		return true
	}
	return false
}

func isCSIHostpathPVKeep(pv *v1.PersistentVolume) bool {
	csiVolAttribsAnnotationKey := "csi.volume.kubernetes.io/volume-attributes"

	if pv == nil || pv.Annotations == nil { // default is keep
		return true
	}

	attribs := make(map[string]string)
	json.Unmarshal([]byte(pv.Annotations[csiVolAttribsAnnotationKey]), attribs)
	if attribs["keep"] == "false" {
		return false
	}
	return true
}

func isKeepPV(pv *v1.PersistentVolume) bool {
	if pv.Spec.HostPath != nil {
		return isHostpathPVKeep(pv)
	} else if pv.Spec.CSI != nil {
		return isCSIHostpathPVKeep(pv)
	}
	return false
}

func (pna *pvNodeAffinity) recordPvcMountToNode(pvname, mountPath, nodename, poduid, podNS, podName string) (bool, error) {
	pv, err := pna.kubeClient.Core().PersistentVolumes().Get(pvname, metav1.GetOptions{})
	if err != nil || pv == nil {
		return false, fmt.Errorf("failed to fetch PV %s from API server. err=%v", pvname, err)
	}
	if pv.Spec.HostPath == nil || pv.Spec.HostPath.Path == "" {
		return false, nil
	}
	if pv.Annotations == nil {
		pv.Annotations = make(map[string]string, 0)
	}
	lastMountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]
	lastCapacityState := pv.Annotations[xfs.PVHostPathCapacityStateAnn]
	changed := false
	if pv.Annotations[xfs.PVHostPathCapacityAnn] != "" { // if set new host path capacity
		capacity, errParse := strconv.ParseInt(pv.Annotations[xfs.PVHostPathCapacityAnn], 10, 64)
		if errParse != nil {
			pv.Annotations[xfs.PVHostPathCapacityStateAnn] = fmt.Sprintf("capacity cannot parse err:%v", errParse)
		} else {
			subid := ""
			if pv.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
				subid = poduid
			}
			find, info := pna.xfsQuotaManager.IsIdXFSQuota(string(pv.UID), subid)
			if find == false { // have not be mounted now
				pv.Annotations[xfs.PVHostPathCapacityStateAnn] = fmt.Sprintf("err to %d:node[%s] has not mount the pv", capacity, nodename)
			} else {
				if info.HardQuota/(1024*1024) != capacity/(1024*1024) { // has change (size:MB)
					_, errChange := pna.xfsQuotaManager.ChangeQuota(string(pv.UID), subid, capacity, capacity)
					if errChange != nil {
						pv.Annotations[xfs.PVHostPathCapacityStateAnn] = fmt.Sprintf("err to %d:%v", capacity, errChange)
					} else {
						pv.Annotations[xfs.PVHostPathCapacityStateAnn] = fmt.Sprintf("ok to %d", capacity)
					}
				} else {
					pv.Annotations[xfs.PVHostPathCapacityStateAnn] = fmt.Sprintf("ok to %d", capacity)
				}
			}
		}
	} else {
		if _, find := pv.Annotations[xfs.PVHostPathCapacityStateAnn]; find == true {
			delete(pv.Annotations, xfs.PVHostPathCapacityStateAnn)
			changed = true
		}
	}
	podInfo := ""
	if isNeedAddPodInfo(pv) {
		podInfo = fmt.Sprintf("%s:%s:%s", podNS, podName, poduid)
	}
	curMountInfo, err := pna.calcPVNodeInfo(lastMountInfo, nodename, string(pv.UID), mountPath, podInfo, isKeepPV(pv))
	if err == nil && (changed || curMountInfo != lastMountInfo ||
		pv.Annotations[xfs.PVHostPathCapacityStateAnn] != lastCapacityState) {
		pv.Annotations[xfs.PVCVolumeHostPathMountNode] = curMountInfo
		_, errUpdate := pna.kubeClient.Core().PersistentVolumes().Update(pv)
		return errUpdate == nil, errUpdate
	} else {
		return false, err
	}
}
func (pna *pvNodeAffinity) sync(poduid, mountPath, pvname, podNS, podName string) {
	_, errrecord := pna.recordPvcMountToNode(pvname, mountPath, pna.nodeName, poduid, podNS, podName)
	if errrecord != nil {
		// glog.Errorf("processPodVolumes pod:%s:%s to pv:%s error=%v", podNS, podName, pvname, errrecord)
	}
}

func mergeMountInfoList(oldList, newList MountInfoList, savePath func(path string) bool) MountInfoList {
	ret := make(MountInfoList, 0, len(oldList)+len(newList))
	newSet := sets.NewString()
	for _, info := range newList {
		if savePath(info.HostPath) { // check if the path is used by any pod
			newSet.Insert(getStandardPathString(info.HostPath))
			ret = append(ret, info)
		}
	}
	for _, info := range oldList {
		if newSet.Has(getStandardPathString(info.HostPath)) == false && savePath(info.HostPath) {
			ret = append(ret, info)
		}
	}
	sort.Sort(ret)
	return ret
}
func getPVQuotaInfo(pvid string, xfsQuotaManager xfs.DiskQuotaManager, mountPath, pinfo string) (mil MountInfoList) {
	infos := xfsQuotaManager.GetXFSQuotasById(pvid)
	for _, info := range infos {
		if getStandardPathString(mountPath) == getStandardPathString(info.GetPath()) ||
			getStandardPathString(mountPath) == getStandardPathString(info.Path) {
			num, size := xfs.GetDirFileSize(info.GetPath())
			mi := MountInfo{
				HostPath:             info.Path,
				VolumeQuotaSize:      info.HardQuota,
				VolumeCurrentSize:    size,
				VolumeCurrentFileNum: num,
			}
			if pinfo != "" {
				mi.PodInfo = &PodInfo{
					Info: pinfo,
				}
			}
			mil = append(mil, mi)
			return
		}
	}
	sort.Sort(mil)
	return mil
}
func getStandardPathString(p string) string {
	return path.Clean(p) + "/"
}
