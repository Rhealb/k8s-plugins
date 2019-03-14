/*
Copyright 2018 The Kubernetes Authors.

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

package hostpath

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	v1 "k8s.io/api/core/v1"
	glog "k8s.io/klog"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"

	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager"
	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager/common"

	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/csi-common"
)

type nodeServer struct {
	*csicommon.DefaultNodeServer
	k8sClient                 k8sInterface
	xfsquotamanager           xfsquotamanager.Interface
	quotaPathUsedByMountPaths map[string]map[string]bool
	pathUsedMu                sync.Mutex
	nodeName                  string
	mount                     mount.Interface
}

func (ns *nodeServer) addPathUsed(quotaPath, mountPath string) {
	ns.pathUsedMu.Lock()
	defer ns.pathUsedMu.Unlock()

	quotaPath = path.Clean(quotaPath)
	mountPath = path.Clean(mountPath)

	mountPaths, _ := ns.quotaPathUsedByMountPaths[quotaPath]
	if mountPaths == nil {
		mountPaths = map[string]bool{}
	}
	mountPaths[mountPath] = true
	ns.quotaPathUsedByMountPaths[quotaPath] = mountPaths
}
func (ns *nodeServer) DeletePathUsed(mountPath string) {
	ns.pathUsedMu.Lock()
	defer ns.pathUsedMu.Unlock()
	ns.deletePathUsed(mountPath)
}
func (ns *nodeServer) deletePathUsed(mountPath string) {
	mountPath = path.Clean(mountPath)
	for quotaPath, mountPaths := range ns.quotaPathUsedByMountPaths {
		if _, exist := mountPaths[mountPath]; exist {
			delete(mountPaths, mountPath)
			if len(mountPaths) == 0 {
				delete(ns.quotaPathUsedByMountPaths, quotaPath)
			} else {
				ns.quotaPathUsedByMountPaths[quotaPath] = mountPaths
			}
			return
		}
	}
}

func (ns *nodeServer) IsQuotaPathUsed(quotaPath string) bool {
	ns.pathUsedMu.Lock()
	defer ns.pathUsedMu.Unlock()
	quotaPath = path.Clean(quotaPath)

	mountPaths, _ := ns.quotaPathUsedByMountPaths[quotaPath]
	if mountPaths == nil || len(mountPaths) == 0 {
		return false
	}
	return true
}

func (ns *nodeServer) DeleteQuotaPathUsed(quotaPath, ownerid string) error {
	ns.pathUsedMu.Lock()
	defer ns.pathUsedMu.Unlock()
	quotaPath = path.Clean(quotaPath)
	if ownerid != "" && ns.k8sClient.IsPodExist(ownerid) == true {
		return fmt.Errorf("cann't DeleteQuotaPathUsed %s , is used by %s\n", quotaPath, ownerid)
	}
	mountPaths, _ := ns.quotaPathUsedByMountPaths[quotaPath]
	if mountPaths != nil && len(mountPaths) > 0 {
		for mp := range mountPaths {
			err := ns.getMounter().Unmount(mp)
			if err != nil {
				return fmt.Errorf("DeleteQuotaPathUsed umount %s on %s err:%v", mp, quotaPath, err)
			}
			ns.deletePathUsed(mp)
		}
	}
	return nil
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	starttime := time.Now()
	targetPath := req.GetTargetPath()
	notMnt, err := ns.getMounter().IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	readOnly := req.GetReadonly()
	volumeId := req.GetVolumeId()
	//	attrib := req.GetVolumeAttributes()
	//mountFlags := req.GetVolumeCapability().GetMount().GetMountFlags()

	//glog.V(4).Infof("target %v\n readonly %v\n volumeId %v\nattributes %v\n mountflags %v\n",
	//	targetPath, readOnly, volumeId, attrib, mountFlags)

	options := []string{"bind"}
	if readOnly {
		options = append(options, "ro")
	}
	podIdFromPath := getPodIdFromMountTargetPath(targetPath)
	if podIdFromPath == "" {
		return nil, status.Error(codes.Internal, fmt.Sprintf("get podid from targetpath %s err", targetPath))
	}
	pv := ns.k8sClient.GetCSIPVByVolumeId(volumeId)
	if pv == nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("volumeid %s pv not find", volumeId))
	}
	recycle := isKeep(map[string]string{}, pv)
	share := isShare(map[string]string{}, pv)
	quotaSize := getQuotaSize(pv)
	var podId string
	if share == false {
		podId = podIdFromPath
	}
	if share == false && recycle == true {
		if err := ns.waitPodHostPathPVMountd(podIdFromPath, 60); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("waitPodHostPathPVMountd err:%v", err))
		}
	}
	if recycle == true {
		if exist, p := ns.hasAnyCanUsedQuotaPath(pv.Annotations); exist == false && p != "" {
			glog.Errorf("quotapath %s is unavailable", p)
			return nil, status.Error(codes.Internal, fmt.Sprintf("quotapath %s is unavailable", p))
		}
	}
	if err := ns.waitSyncToCache(podIdFromPath, 3*time.Second); err != nil {
		return nil, fmt.Errorf("wait pod %s err:%v", podIdFromPath, err)
	}
	if ok, path, err := ns.xfsquotamanager.AddQuotaPath(volumeId, podId, quotaSize, quotaSize, recycle); ok == false || err != nil {
		glog.V(1).Infof("AddQuotaPath volumeId:%s, podId:%s, podIdFromPath:%s, quotaSize:%d, ok:%t, err:%v", volumeId, podId, podIdFromPath, quotaSize, ok, err)
		return nil, status.Error(codes.Internal, err.Error())
	} else {
		if path == "" {
			glog.V(1).Infof("AddQuotaPath volumeId:%s, podId:%s, podIdFromPath:%s quotaSize:%d, path is empty", volumeId, podId, podIdFromPath, quotaSize)
			return nil, status.Error(codes.Internal, "quota path is empty")
		}
		if err := ns.getMounter().Mount(path, targetPath, "", options); err != nil {
			umounterr := ns.getMounter().Unmount(targetPath)
			glog.V(1).Infof("AddQuotaPath volumeId:%s, podId:%s, podIdFromPath:%s quotaSize:%d, mount err:%v, umounterr:%v", volumeId, podId, podIdFromPath, quotaSize, err, umounterr)
			return nil, err
		}
		ns.addPathUsed(path, targetPath)
		ns.xfsquotamanager.AddQuotaPathMountPath(path, targetPath)
		glog.V(1).Infof("AddQuotaPath volumeId:%s, podId:%s, podIdFromPath:%s, quotaSize:%d, mount %s success, usetime:%v", volumeId, podId, podIdFromPath, quotaSize, path, time.Since(starttime))
	}
	//ns.k8sClient.SetPodExist(podId, true)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	notMnt, err := ns.getMounter().IsLikelyNotMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

umount:
	err = ns.getMounter().Unmount(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt, err := ns.getMounter().IsLikelyNotMountPoint(targetPath); err == nil && notMnt == false {
		goto umount
	}

	ns.DeletePathUsed(targetPath)
	ns.xfsquotamanager.RemoveMountPath(targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *nodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *nodeServer) waitSyncToCache(podId string, timeout time.Duration) error {
	startTime := time.Now()
	for {
		if pod, err := ns.k8sClient.GetPodByPodId(podId); pod != nil && err == nil {
			return nil
		}
		if time.Since(startTime) > timeout {
			return fmt.Errorf("time out")
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (ns *nodeServer) getMounter() mount.Interface {
	if ns.mount == nil {
		ns.mount = mount.New("")
	}
	return ns.mount
}

func (ns *nodeServer) hasAnyCanUsedQuotaPath(pvAnn map[string]string) (exist bool, unavailablePath string) {
	if pvAnn == nil || pvAnn[common.PVVolumeHostPathMountNode] == "" {
		return false, ""
	}
	mountInfo := pvAnn[common.PVVolumeHostPathMountNode]
	hppmil := HostPathPVMountInfoList{}
	err := json.Unmarshal([]byte(mountInfo), &hppmil)
	if err != nil {
		return false, ""
	}
	unexistPath := []string{}
	for _, mn := range hppmil {
		if mn.NodeName == ns.nodeName {
			for _, info := range mn.MountInfos {
				exist, used, _ := ns.xfsquotamanager.IsQuotaPathExistAndUsed(info.HostPath)
				if exist == true && used == false {
					return true, ""
				}
				if exist == false {
					unexistPath = append(unexistPath, info.HostPath)
				}
			}
			break
		}
	}
	if len(unexistPath) > 0 {
		return false, strings.Join(unexistPath, ",")
	}
	return false, ""
}

// TODO: this code should be removed after xfsquotamanager in kubelet was removed
func (ns *nodeServer) waitPodHostPathPVMountd(podId string, timeOut int) error {
	for i := 0; i < timeOut; i++ {
		if ns.isPodHostPathPVMountd(podId) == true {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("wait pod %s hostpathpv mounted timeout", podId)
}

func (ns *nodeServer) isPodHostPathPVMountd(podId string) bool {
	if pod, err := ns.k8sClient.GetPodByPodId(podId); err != nil || pod == nil {
		return false
	} else {
		for _, podVolume := range pod.Spec.Volumes {
			if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
				pvc, errPVC := ns.k8sClient.GetPVCByName(pod.Namespace, pvcSource.ClaimName)
				if errPVC != nil || pvc.Status.Phase != v1.ClaimBound {
					glog.Errorf("pod %s pvc %s is not bound pvc:%v", podId, pvcSource.ClaimName, pvc)
					return false
				}
				pv, errPV := ns.k8sClient.GetPVByName(pvc.Spec.VolumeName)
				if errPV != nil {
					glog.Errorf("pod %s get pv %s err:%v", podId, pvc.Spec.VolumeName, errPV)
					return false
				}
				if pv.Spec.HostPath == nil { // only consider hostpath pv
					continue
				}
				if ok := ns.isHostPathPVMountedByPod(string(pv.UID)); ok == false {
					glog.Infof("pod %s pv %s is not mounted", podId, pv.Name)
					return false
				}

			}
		}
		return true
	}
}

func (ns *nodeServer) isHostPathPVMountedByPod(volumeId string) bool {
	diskInfos := ns.xfsquotamanager.GetQuotaDiskInfos()
	for _, disk := range diskInfos {
		for _, info := range disk.PathQuotaInfos {
			if info.VolumeId == volumeId {
				return true
			}
		}
	}
	return false
}

func isKeep(attrib map[string]string, pv *v1.PersistentVolume) bool {
	if pv.Annotations != nil && pv.Annotations[common.PVHostPathMountPolicyAnn] != "" { // default use annotation
		return pv.Annotations[common.PVHostPathMountPolicyAnn] != common.PVHostPathNone
	}
	if value, exist := attrib["keep"]; exist == false || value == "true" {
		return true
	}
	return false
}

func isShare(attrib map[string]string, pv *v1.PersistentVolume) bool {
	if pv.Annotations != nil && pv.Annotations[common.PVHostPathQuotaForOnePod] != "" { // default use annotation
		return pv.Annotations[common.PVHostPathQuotaForOnePod] == "false"
	}
	if value, _ := attrib["foronepod"]; value == "false" {
		return true
	}
	return false
}

func getQuotaSize(pv *v1.PersistentVolume) int64 {
	defaultQuotaSize := int64(100 * 1024 * 1024) // 100MB
	if pv == nil {
		return defaultQuotaSize
	}
	if pv.Annotations != nil && pv.Annotations[common.PVHostPathCapacityAnn] != "" {
		capacityAnn, errParse := strconv.ParseInt(pv.Annotations[common.PVHostPathCapacityAnn], 10, 64)
		if errParse == nil {
			return capacityAnn
		}
	}
	storage := pv.Spec.Capacity[v1.ResourceStorage]
	return storage.Value()
}

func getPodIdFromMountTargetPath(mountPath string) string {
	reg := regexp.MustCompile("[0-9a-z]+-[0-9a-z]+-[0-9a-z]+-[0-9a-z]+-[0-9a-z]+")
	strs := reg.FindAllString(mountPath, 1)
	if len(strs) == 1 {
		return strs[0]
	}
	return ""
}
