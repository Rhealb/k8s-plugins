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

package predicates

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	//"k8s.io/kubernetes/pkg/api"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

type PersistentVolumeStore interface {
	GetPersistentVolumeInfo(pvID string) (*v1.PersistentVolume, error)
	List() (v1.PersistentVolumeList, error)
}

type PersistentVolumeClaimStore interface {
	GetPersistentVolumeClaimInfo(namespace string, name string) (*v1.PersistentVolumeClaim, error)
}

type PodHostPVDiskChecker struct {
	pvInfo  PersistentVolumeInfo
	pvcInfo PersistentVolumeClaimInfo
}

func NewPodHostPVDiskPredicate(pvInfo PersistentVolumeInfo, pvcInfo PersistentVolumeClaimInfo) algorithm.FitPredicate {
	checker := &PodHostPVDiskChecker{
		pvInfo:  pvInfo,
		pvcInfo: pvcInfo,
	}
	return checker.CheckPodHostPathPVDiskRequest
}

type DiskInfo struct {
	path     string
	size     int64
	disabled bool
}
type DiskInfoList []DiskInfo

func (l DiskInfoList) Len() int { return len(l) }
func (l DiskInfoList) Less(i, j int) bool {
	if l[i].size != l[j].size {
		return l[i].size < l[j].size
	} else {
		return l[i].path < l[j].path
	}
}
func (l DiskInfoList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

func (phpvdc *PodHostPVDiskChecker) hasNodeMountedToPV(pv *v1.PersistentVolume, nodename string) bool {
	if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
		return false
	}
	mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
	if errUmarshal != nil {

		return false
	}

	for _, mount := range mountList {
		if mount.NodeName == nodename {
			return true
		}
	}
	return false
}
func (phpvdc *PodHostPVDiskChecker) getPodHostPathPV(pod *v1.Pod, nodename string) (int64, DiskInfoList, error) {
	list := make(DiskInfoList, 0)
	var sumSize int64
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			pvc, err := phpvdc.pvcInfo.GetPersistentVolumeClaimInfo(pod.Namespace, pvcSource.ClaimName)
			if err != nil || pvc == nil {
				return 0, nil, fmt.Errorf("get pvc %s error:%v", pvcSource.ClaimName, err)
			}

			pv, err := phpvdc.pvInfo.GetPersistentVolumeInfo(pvc.Spec.VolumeName)
			if err != nil || pv == nil {
				return 0, nil, fmt.Errorf("failed to fetch PV %q from API server. err=%v", pvc.Spec.VolumeName, err)
			}

			if pv.Spec.HostPath == nil && isHostPathCSIPV(pv) == false {
				continue
			}
			if isSharePV(pv) == false || phpvdc.hasNodeMountedToPV(pv, nodename) == false {
				storage, ok := pv.Spec.Capacity[v1.ResourceStorage]
				capacity := storage.Value()
				if pv.Annotations != nil && pv.Annotations[xfs.PVHostPathCapacityAnn] != "" {
					capacityAnn, errParse := strconv.ParseInt(pv.Annotations[xfs.PVHostPathCapacityAnn], 10, 64)
					if errParse == nil {
						capacity = capacityAnn
					}
				}
				if ok {
					list = append(list, DiskInfo{
						path: "",
						size: capacity,
					})
					sumSize += capacity
				} else {
					return 0, nil, fmt.Errorf("pv has not define capacity")
				}
			}
		}
	}
	sort.Sort(list)
	return sumSize, list, nil
}

func isHostPathCSIPV(pv *v1.PersistentVolume) bool {
	if pv.Spec.CSI != nil && strings.Contains(strings.ToLower(pv.Spec.CSI.Driver), "hostpath") == true {
		return true
	}
	return false
}

func isSharePV(pv *v1.PersistentVolume) bool {
	if pv.Annotations == nil {
		return false
	}
	if pv.Spec.HostPath != nil {
		return pv.Annotations[xfs.PVHostPathQuotaForOnePod] != "true"
	} else if isHostPathCSIPV(pv) == true {
		csiVolAttribsAnnotationKey := "csi.volume.kubernetes.io/volume-attributes"
		attribs := make(map[string]string)
		json.Unmarshal([]byte(pv.Annotations[csiVolAttribsAnnotationKey]), attribs)
		return attribs["foronepod"] == "false"
	}
	return false
}

func reduceNodeQuotaSize(list DiskInfoList, info pvnodeaffinity.MountInfo) {
	for i, _ := range list {
		if strings.HasPrefix(info.HostPath, list[i].path) {
			list[i].size -= info.VolumeQuotaSize
			if list[i].size < 0 {
				list[i].size = 0
			}
			return
		}
	}
}
func (phpvdc *PodHostPVDiskChecker) caculateNodeQuotaInfo(list DiskInfoList, nodename string) {
	pvs, err := phpvdc.pvInfo.List()
	if err != nil {
		return
	}
	for _, pv := range pvs {
		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			continue
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			continue
		}

		for _, mount := range mountList {
			if mount.NodeName == nodename {
				for _, mountPathInfo := range mount.MountInfos {
					reduceNodeQuotaSize(list, mountPathInfo)
				}
				break
			}
		}
	}
}

type podQuotaInfo struct {
	podNS, podName string
	mountPath      string
	quotaSize      int64
	pvName         string
}

func getPodInfo(mi pvnodeaffinity.MountInfo) (podNs, podName string) {
	if mi.PodInfo != nil && mi.PodInfo.Info != "" {
		strs := strings.Split(mi.PodInfo.Info, ":")
		if len(strs) == 3 {
			return strs[0], strs[1]
		}
	}
	return "", ""
}
func podKey(ns, name string) string {
	return ns + "/" + name
}
func (phpvdc *PodHostPVDiskChecker) caculateNodePVQuotaInfo(nodename string) (map[string]podQuotaInfo, error) {
	ret := make(map[string]podQuotaInfo, 0)
	pvs, err := phpvdc.pvInfo.List()
	if err != nil {
		return ret, err
	}

	for _, pv := range pvs {
		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			continue
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			continue
		}

		for _, mount := range mountList {
			if mount.NodeName == nodename {
				var unKnowCount int
				for _, mountPathInfo := range mount.MountInfos {
					podNS, podName := getPodInfo(mountPathInfo)
					if podNS != "" && podName != "" {
						ret[podKey(podNS, podName)+pv.Name] = podQuotaInfo{
							podNS:     podNS,
							podName:   podName,
							mountPath: mountPathInfo.HostPath,
							quotaSize: mountPathInfo.VolumeQuotaSize,
							pvName:    pv.Name,
						}
					} else {
						unKnowCount++
						unKnowName := fmt.Sprintf("unknow%d", unKnowCount)
						ret[unKnowName+pv.Name] = podQuotaInfo{
							podNS:     "unknow",
							podName:   "unknow",
							mountPath: mountPathInfo.HostPath,
							quotaSize: mountPathInfo.VolumeQuotaSize,
							pvName:    pv.Name,
						}
					}
				}
				break
			}
		}
	}
	return ret, nil
}

func (phpvdc *PodHostPVDiskChecker) getNodeDiskInfo(node *v1.Node, nodePods []*v1.Pod, schedulerPod *v1.Pod) (int64, DiskInfoList, bool, error) {
	// step 1: get node quota disk capacity
	if node.Annotations == nil || node.Annotations[xfs.NodeDiskQuotaInfoAnn] == "" {
		return 0, nil, false, nil
	}
	nodeDiskQuotaInfoList := make(xfs.NodeDiskQuotaInfoList, 0)
	err := json.Unmarshal([]byte(node.Annotations[xfs.NodeDiskQuotaInfoAnn]), &nodeDiskQuotaInfoList)
	if err != nil {
		return 0, nil, false, fmt.Errorf("getNodeDiskInfo Unmarshal NodeDiskQuotaInfoAnn err:%v", err)
	}
	diskInfoMap := make(map[string]*DiskInfo)
	for _, disk := range nodeDiskQuotaInfoList {
		diskInfoMap[disk.MountPath] = &DiskInfo{
			path:     disk.MountPath,
			size:     disk.Allocable,
			disabled: disk.Disabled,
		}
	}

	// step 2: reduce the used quota by pod
	pvQuotaInfo, err := phpvdc.caculateNodePVQuotaInfo(node.Name)
	if err != nil {
		return 0, nil, false, fmt.Errorf("getNodeDiskInfo caculateNodePVQuotaInfo err:%v", err)
	}
	for path, diskInfo := range diskInfoMap {
		for _, podQuotaInfo := range pvQuotaInfo {
			if strings.HasPrefix(podQuotaInfo.mountPath, path) {
				diskInfo.size -= podQuotaInfo.quotaSize
				if diskInfo.size < 0 {
					diskInfo.size = 0
				}
			}
		}
	}

	// step 3: reduce the pod used quota which is not recorded in pv yet
	nodePVCreateNum := make(map[string]uint)
	for _, podQuotaInfo := range pvQuotaInfo {
		count, _ := nodePVCreateNum[podQuotaInfo.pvName]
		nodePVCreateNum[podQuotaInfo.pvName] = count + 1
	}

	notRecordPods := make([]*v1.Pod, 0, len(nodePods))
	for _, pod := range nodePods {
		if pod == nil {
			continue
		}
		if phpvdc.isPodInPVHistory(pod, pvQuotaInfo) { // this pod is reduced at step 2
			phpvdc.syncNodePVUseNum(pod, nodePVCreateNum)
			continue
		}
		notRecordPods = append(notRecordPods, pod)
	}
	var sumSize int64
	for _, pod := range notRecordPods {
		size, _ := phpvdc.getPodWillUsedQuotaSize(pod, nodePVCreateNum, true)
		sumSize += size
	}

	size, _ := phpvdc.getPodWillUsedQuotaSize(schedulerPod, nodePVCreateNum, false)
	if size == 0 { // size == 0 means the pod can run directly
		return 0, nil, true, nil
	}

	list := make(DiskInfoList, 0, len(diskInfoMap))
	var capacity int64
	for _, diskInfo := range diskInfoMap {
		if diskInfo.disabled == true {
			continue
		}
		capacity += diskInfo.size
		info := DiskInfo{
			path: diskInfo.path,
			size: diskInfo.size - sumSize,
		}
		if info.size < 0 {
			info.size = 0
		}
		list = append(list, info)
	}
	capacity -= sumSize
	return capacity, list, false, nil
}
func (phpvdc *PodHostPVDiskChecker) syncNodePVUseNum(pod *v1.Pod, nodePVUseNum map[string]uint) {
	if pod == nil {
		return
	}
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			pv, err := getHostPathPVByPVCName(phpvdc.pvInfo, phpvdc.pvcInfo, pod.Namespace, pvcSource.ClaimName)
			if err != nil || pv == nil {
				continue
			}
			if isPVForOnePod(pv) == true && isPVKeepForNode(pv) == true {
				if num, _ := nodePVUseNum[pv.Name]; num > 0 {
					nodePVUseNum[pv.Name] = num - 1
				}
			}
		}
	}
}
func (phpvdc *PodHostPVDiskChecker) isPodInPVHistory(pod *v1.Pod, quotMap map[string]podQuotaInfo) bool {
	if pod == nil {
		return false
	}
	pk := podKey(pod.Namespace, pod.Name)
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			pv, err := getHostPathPVByPVCName(phpvdc.pvInfo, phpvdc.pvcInfo, pod.Namespace, pvcSource.ClaimName)
			if err != nil || pv == nil {
				continue
			}
			if _, exist := quotMap[pk+pv.Name]; exist == false {
				return false
			}

		}
	}
	return true
}

func (phpvdc *PodHostPVDiskChecker) getPodWillUsedQuotaSize(pod *v1.Pod, nodePVUseNum map[string]uint, update bool) (size int64, err error) {
	if pod == nil {
		return 0, nil
	}
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			pv, err := getHostPathPVByPVCName(phpvdc.pvInfo, phpvdc.pvcInfo, pod.Namespace, pvcSource.ClaimName)
			if err != nil || pv == nil {
				continue
			}
			if isPVForOnePod(pv) == true && isPVKeepForNode(pv) == true {
				if num, _ := nodePVUseNum[pv.Name]; num > 0 {
					if update {
						nodePVUseNum[pv.Name] = num - 1
					}
					continue
				}
			}
			if isPVForOnePod(pv) {
				size += getPVCapacity(pv)
			} else if _, exist := nodePVUseNum[pv.Name]; exist == false { // the quota path is created by other pod
				size += getPVCapacity(pv)
				nodePVUseNum[pv.Name] = 1
			}
		}
	}
	return
}

func getPVCapacity(pv *v1.PersistentVolume) int64 {
	if pv == nil {
		return 0
	}
	storage, exists := pv.Spec.Capacity[v1.ResourceStorage]
	if exists == false {
		return 0
	}
	capacity := storage.Value()
	if pv.Annotations != nil &&
		pv.Annotations[xfs.PVHostPathCapacityAnn] != "" {
		capacityAnn, errParse := strconv.ParseInt(pv.Annotations[xfs.PVHostPathCapacityAnn], 10, 64)
		if errParse == nil {
			capacity = capacityAnn
		}
	}
	return capacity
}
func isPVKeepForNode(pv *v1.PersistentVolume) bool {
	if pv != nil && pv.Annotations != nil &&
		pv.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathNone {
		return false
	}
	return true
}
func isPVForOnePod(pv *v1.PersistentVolume) bool {
	if pv != nil && pv.Annotations != nil &&
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
		return true
	}
	return false
}
func canRequestMatch(request, haveNow DiskInfoList) bool {
	var ir, ih int
	for {
		if ir >= len(request) || ih >= len(haveNow) {
			break
		}
		if request[ir].size <= haveNow[ih].size {
			haveNow[ih].size -= request[ir].size
			ir += 1
			continue
		} else {
			ih += 1
			continue
		}
	}
	if ir >= len(request) && ih >= len(haveNow) {
		return true
	} else if ir >= len(request) {
		return true
	}
	return false
}
func (phpvdc *PodHostPVDiskChecker) CheckPodHostPathPVDiskRequest(pod *v1.Pod, meta algorithm.PredicateMetadata, nodeInfo *schedulercache.NodeInfo) (bool, []algorithm.PredicateFailureReason, error) {
	node := nodeInfo.Node()
	if node == nil {
		return false, nil, fmt.Errorf("node not found")
	}
	podRequestSize, podRequestList, errPod := phpvdc.getPodHostPathPV(pod, node.Name)
	if errPod != nil {
		return false, []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure}, nil
	}
	nodeDiskSize, nodeDiskList, runDirectly, errNode := phpvdc.getNodeDiskInfo(node, nodeInfo.Pods(), pod)
	if errNode != nil {
		return false, []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure}, nil
	}
	if runDirectly {
		return true, nil, nil
	}
	//phpvdc.caculateNodeQuotaInfo(nodeDiskList, node.Name)
	sort.Sort(nodeDiskList)
	if nodeDiskSize < podRequestSize {
		return false, []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure}, nil
	}
	if canRequestMatch(podRequestList, nodeDiskList) == false {
		return false, []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure}, nil
	}
	return true, nil, nil
}

func calcUsePVCPodsCount(pods []*v1.Pod, pvcName string) int {
	if pods == nil || len(pods) == 0 || pvcName == "" {
		return 0
	}
	var count int
	for _, pod := range pods {
		for _, podVolume := range pod.Spec.Volumes {
			if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
				if pvcSource.ClaimName == pvcName {
					count++
				}
			}
		}
	}
	return count
}

func isPodActive(p *v1.Pod) bool {
	return v1.PodSucceeded != p.Status.Phase &&
		v1.PodFailed != p.Status.Phase &&
		p.DeletionTimestamp == nil
}

func filterNamespaceActivePod(pods []*v1.Pod, ns string) []*v1.Pod {
	ret := make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod.Namespace == ns && isPodActive(pod) == true {
			ret = append(ret, pod)
		}
	}
	return ret
}

func calcNodeRunPVCPodCount(pods []*v1.Pod, pvcName string) map[string]int {
	ret := make(map[string]int)
	if pods == nil || len(pods) == 0 || pvcName == "" {
		return ret
	}
	for _, pod := range pods {
		for _, podVolume := range pod.Spec.Volumes {
			if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
				if pvcSource.ClaimName == pvcName {
					node := pod.Spec.NodeName
					nodeCount, _ := ret[node]
					ret[node] = nodeCount + 1
				}
			}
		}
	}
	return ret
}

func getHostPathPVByPVCName(pvInfo PersistentVolumeInfo, pvcInfo PersistentVolumeClaimInfo, namespace, pvcName string) (*v1.PersistentVolume, error) {
	pvc, err := pvcInfo.GetPersistentVolumeClaimInfo(namespace, pvcName)
	if err != nil || pvc == nil {
		return nil, fmt.Errorf("failed to fetch PVC %s/%s from API server. err=%v", namespace, pvcName, err)
	}

	pv, err := pvInfo.GetPersistentVolumeInfo(pvc.Spec.VolumeName)
	if err != nil || pv == nil {
		return nil, fmt.Errorf("failed to fetch PV %q from API server. err=%v", pvc.Spec.VolumeName, err)
	}

	if pv.Spec.HostPath == nil && isHostPathCSIPV(pv) == false {
		return nil, nil
	}
	return pv, nil
}

//HostPathPVAffinityPredicate
type HostPathPVAffinityPredicate struct {
	pvInfo    PersistentVolumeInfo
	pvcInfo   PersistentVolumeClaimInfo
	podLister algorithm.PodLister
}

func NewHostPathPVAffinityPredicate(pvInfo PersistentVolumeInfo, pvcInfo PersistentVolumeClaimInfo, podLister algorithm.PodLister) algorithm.FitPredicate {
	checker := &HostPathPVAffinityPredicate{
		pvInfo:    pvInfo,
		pvcInfo:   pvcInfo,
		podLister: podLister,
	}
	return checker.CheckHostPathPVAffinity
}
func (hpap *HostPathPVAffinityPredicate) CheckHostPathPVAffinity(pod *v1.Pod, meta algorithm.PredicateMetadata, nodeInfo *schedulercache.NodeInfo) (bool, []algorithm.PredicateFailureReason, error) {
	node := nodeInfo.Node()
	if node == nil {
		return false, nil, fmt.Errorf("node not found")
	}
	pods, listError := hpap.podLister.List(labels.Everything())
	if listError != nil {
		return false, nil, fmt.Errorf("list pod error")
	}
	allPods := filterNamespaceActivePod(pods, pod.Namespace)
	for _, podVolume := range pod.Spec.Volumes {
		ok, _, set, err := hpap.getHostPathPVMountedNodes(podVolume, pod.Namespace, allPods)
		if err != nil {
			return false, []algorithm.PredicateFailureReason{ErrHostPathPVAffinityPredicate}, nil
		}
		if ok == false || len(set) == 0 {
			continue
		}
		if set.Has(node.Name) == false {
			return false, []algorithm.PredicateFailureReason{ErrHostPathPVAffinityPredicate}, nil
		}
	}
	return true, nil, nil
}

func (hpap *HostPathPVAffinityPredicate) getHostPathPVMountedNodes(podVolume v1.Volume,
	podNamespace string, allPods []*v1.Pod) (bool, string, sets.String, error) {
	ret := sets.NewString()
	if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
		pv, err := getHostPathPVByPVCName(hpap.pvInfo, hpap.pvcInfo, podNamespace, pvcSource.ClaimName)
		if pv == nil || err != nil {
			return false, "", ret, err
		}

		if pv.Annotations == nil ||
			pv.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathNone || // default policy keep and false
			pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" ||
			pv.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
			if pv.Annotations != nil &&
				pv.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathKeep &&
				pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" &&
				pv.Annotations[xfs.PVHostPathQuotaForOnePod] == "false" {

				nodes := calcNodeRunPVCPodCount(allPods, pvcSource.ClaimName)
				for node, count := range nodes {
					if count > 0 {
						ret.Insert(node)
					}
				}
			}
			return true, pv.Name, ret, nil
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			return true, pv.Name, ret, fmt.Errorf("getPodPVNodeMountInfos Unmarshal[%s] error %v", mountInfo, errUmarshal)
		}

		for _, mount := range mountList {
			ret.Insert(mount.NodeName)
		}
		return true, pv.Name, ret, nil
	} else {
		return false, "", ret, nil
	}
}

//HostPathPVAffinityPredicate2
type HostPathPVAffinityPredicate2 struct {
	pvInfo    PersistentVolumeInfo
	pvcInfo   PersistentVolumeClaimInfo
	podLister algorithm.PodLister
}

func NewHostPathPVAffinityPredicate2(pvInfo PersistentVolumeInfo, pvcInfo PersistentVolumeClaimInfo, podLister algorithm.PodLister) algorithm.FitPredicate {
	checker := &HostPathPVAffinityPredicate2{
		pvInfo:    pvInfo,
		pvcInfo:   pvcInfo,
		podLister: podLister,
	}
	return checker.CheckHostPathPVAffinity
}
func (hpap2 *HostPathPVAffinityPredicate2) getKeepAndForOnePodPVHostPaths(podVolume v1.Volume,
	podNamespace string) (isOk bool, pvcName string, nodePathInfo map[string]int, err error) {
	ret := make(map[string]int)
	if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
		pv, err := getHostPathPVByPVCName(hpap2.pvInfo, hpap2.pvcInfo, podNamespace, pvcSource.ClaimName)
		if pv == nil {
			return false, pvcSource.ClaimName, ret, err
		}
		if pv.Annotations == nil ||
			pv.Annotations[xfs.PVHostPathMountPolicyAnn] != xfs.PVHostPathKeep ||
			pv.Annotations[xfs.PVHostPathQuotaForOnePod] != "true" {
			return false, pvcSource.ClaimName, ret, nil
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			return true, pvcSource.ClaimName, ret, fmt.Errorf("getKeepAndForOnePodPVHostPaths Unmarshal[%s] error %v", mountInfo, errUmarshal)
		}
		for _, mount := range mountList {
			ret[mount.NodeName] = len(mount.MountInfos)
		}
		return true, pvcSource.ClaimName, ret, nil
	} else {
		return false, "", ret, nil
	}
}

func (hpap2 *HostPathPVAffinityPredicate2) CheckHostPathPVAffinity(pod *v1.Pod, meta algorithm.PredicateMetadata, nodeInfo *schedulercache.NodeInfo) (bool, []algorithm.PredicateFailureReason, error) {
	node := nodeInfo.Node()
	if node == nil {
		return false, nil, fmt.Errorf("node not found")
	}
	pods, listError := hpap2.podLister.List(labels.Everything())
	if listError != nil {
		return false, nil, fmt.Errorf("list pod error")
	}
	allPods := filterNamespaceActivePod(pods, pod.Namespace)
	for _, podVolume := range pod.Spec.Volumes {
		isOk, pvcName, canUsePVNodeInfo, err := hpap2.getKeepAndForOnePodPVHostPaths(podVolume, pod.Namespace)
		if err != nil || isOk == false {
			continue
		}

		usedPVNodeInfo := calcNodeRunPVCPodCount(allPods, pvcName)

		isAllUsed := true
		for canUseNode, canUseCount := range canUsePVNodeInfo {
			usedCount, _ := usedPVNodeInfo[canUseNode]
			if canUseCount > usedCount {
				isAllUsed = false
				break
			}
		}

		if isAllUsed == true { // if all history pv hostpaths are used
			continue
		}

		curNodeCanUsedCount, _ := canUsePVNodeInfo[node.Name]
		curNodeUsedCount, _ := usedPVNodeInfo[node.Name]

		if curNodeCanUsedCount <= curNodeUsedCount {
			return false, []algorithm.PredicateFailureReason{ErrHostPathPVAffinityPredicate2}, nil
		}
	}
	return true, nil, nil
}
