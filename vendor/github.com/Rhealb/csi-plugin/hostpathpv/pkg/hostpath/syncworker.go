package hostpath

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager"
	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager/common"

	v1 "k8s.io/api/core/v1"
	glog "k8s.io/klog"
	"k8s.io/kubernetes/pkg/util/mount"
)

type IsQuotPathUseInterface interface {
	IsQuotaPathUsed(quotaPath string) bool
	DeleteQuotaPathUsed(quotaPath, ownerid string) error
}
type SyncWorker struct {
	diskInfos              []xfsquotamanager.DiskQuotaInfo
	k8sClient              k8sInterface
	xfsquotamanager        xfsquotamanager.Interface
	isQuotPathUseInterface IsQuotPathUseInterface
	driverName             string
	nodeName               string
	shouldDeleteQuotaPaths map[string]bool
	tl                     *common.TimeOutLog
}

func resetOrReuseTimer(t *time.Timer, d time.Duration, sawTimeout bool) *time.Timer {
	if t == nil {
		return time.NewTimer(d)
	}
	if !t.Stop() && !sawTimeout {
		<-t.C
	}
	t.Reset(d)
	return t
}
func JitterUntil(f func(), period time.Duration, stopCh <-chan struct{}) {
	var t *time.Timer
	var sawTimeout bool

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		jitteredPeriod := period
		f()
		t = resetOrReuseTimer(t, jitteredPeriod, sawTimeout)

		// NOTE: b/c there is no priority selection in golang
		// it is possible for this to race, meaning we could
		// trigger t.C and stopCh, and t.C select falls through.
		// In order to mitigate we re-check stopCh at the beginning
		// of every loop to prevent extra executions of f().
		select {
		case <-stopCh:
			return
		case <-t.C:
			sawTimeout = true
		}
	}
}

func (r *SyncWorker) Start(period time.Duration) {
	stopCh := make(chan struct{})
	r.tl = common.NewTimeOutLog("SyncWorker")
	go func() {
		JitterUntil(r.populatorLoopFunc(), period, stopCh)
	}()
}

func (r *SyncWorker) populatorLoopFunc() func() {
	return func() {
		r.tl.Start(10*time.Second, 30)
		r.tl.Log("populatorLoopFunc start")
		r.diskInfos = r.xfsquotamanager.GetQuotaDiskInfos()
		r.tl.Log(fmt.Sprintf("populatorLoopFunc GetQuotaDiskInfos len:%d", len(r.diskInfos)))
		if err := r.cleanOrphanQuotaPath(); err != nil {
			glog.Errorf("cleanOrphanQuotaPath err:%v", err)
		}
		r.tl.Log("populatorLoopFunc start syncQuotaPathUsage")
		if err := r.syncQuotaPathUsage(); err != nil {
			glog.Errorf("syncQuotaPathUsage err:%v", err)
		}
		r.tl.Log("populatorLoopFunc start syncNodeQuotaStatus")
		if err := r.syncNodeQuotaStatus(); err != nil {
			glog.Errorf("syncNodeQuotaStatus err:%v", err)
		}
		r.tl.Log("populatorLoopFunc start updatePVQuotaByCapacity")
		if err := r.updatePVQuotaByCapacity(); err != nil {
			glog.Errorf("updatePVQuotaByCapacity err:%v", err)
		}
		r.tl.Stop()
	}
}

func getNodePath(node, p string) string {
	return fmt.Sprintf("%s:%s", node, path.Clean(p))
}

func (r *SyncWorker) updatePVQuotaByCapacity() error {
	pvMap := make(map[string]*v1.PersistentVolume)
	csiPVs, err1 := r.k8sClient.ListCSIPV(r.driverName)
	if err1 != nil {
		return fmt.Errorf("ListCSIPV driver %s err:%v", r.driverName, err1)
	}
	for _, pv := range csiPVs {
		pvMap[getPVVolumeId(pv)] = pv
	}
	updateStates := make(map[string][]PVScaleState)
	for _, disk := range r.diskInfos {
		for _, info := range disk.PathQuotaInfos {
			if info.IsCSIQuotaPath == false {
				continue
			}
			if pv, _ := pvMap[info.VolumeId]; pv != nil {
				shouldQuota := getQuotaSize(pv)
				if (shouldQuota / (1024 * 1024)) != (info.HardQuota / (1024 * 1024)) { // only check to MB
					var state PVScaleState
					state.NodePath = getNodePath(r.nodeName, info.Path)
					state.ToCapacity = shouldQuota

					r.tl.Log(fmt.Sprintf("updatePVQuotaByCapacity set %s quota to %d start", info.Path, shouldQuota))
					if ok, err := r.xfsquotamanager.ChangeQuotaPathQuota(info.ProjectId, shouldQuota, shouldQuota); err != nil || ok == false {
						glog.Errorf("change pv %s quota project %d from %d to %d fail err:%v", pv.Name, info.ProjectId, info.HardQuota, shouldQuota, err)
						state.ScaleSuccess = false
						state.Error = err.Error()
					} else {
						glog.V(1).Infof("change pv %s quota project %d from %d to %d success", pv.Name, info.ProjectId, info.HardQuota, shouldQuota)
						state.ScaleSuccess = true
						state.Error = ""
					}
					r.tl.Log(fmt.Sprintf("updatePVQuotaByCapacity set %s quota to %d stop", info.Path, shouldQuota))
					updateStates[info.VolumeId] = append(updateStates[info.VolumeId], state)
				}
			}
		}
	}
	for pvUid, states := range updateStates {
		if len(states) == 0 {
			continue
		}
		if pv, exist := pvMap[pvUid]; exist && pv != nil {
			for tryTime := 0; tryTime < 3; tryTime++ {
				curPv, err := r.k8sClient.GetPVByName(pv.Name)
				if err != nil {
					glog.Errorf("updatePVQuotaByCapacity set scale state get pv %s err:%v", pv.Name, err)
				} else {
					pvStates := GetPVScaleStates(curPv)
					if _, exist := pvStates[r.nodeName]; exist == false {
						pvStates[r.nodeName] = make(map[string]PVScaleState)
					}
					allOk := true
					for _, s := range states {
						newState := s
						newState.NodePath = ""
						if s.ScaleSuccess == false {
							allOk = false
						}
						pvStates[r.nodeName][s.NodePath] = newState
					}
					if allOk == true {
						pvStates[r.nodeName] = map[string]PVScaleState{
							"*": PVScaleState{ScaleSuccess: true, ToCapacity: states[0].ToCapacity},
						}
					} else {
						delete(pvStates[r.nodeName], "*")
					}
					if changed := SetPVScaleStates(curPv, pvStates); changed {
						err = r.k8sClient.UpdatePV(curPv)
						if err == nil {
							break
						}
					} else {
						break
					}
				}
				time.Sleep(100 * time.Microsecond)
			}
		} else {
			glog.Errorf("pvuid %s is not in pvMap", pvUid)
		}
	}

	return nil
}

func (r *SyncWorker) syncNodeQuotaStatus() error {
	node, errGet := r.k8sClient.GetNodeByName(r.nodeName)
	if errGet != nil {
		return fmt.Errorf("GetNodeByName %s err:%v", r.nodeName, errGet)
	}
	// set disk disable
	disabled := make(map[string]bool)
	for _, disabledisk := range r.getNodeQuotadiskDisableList(node) {
		r.tl.Log(fmt.Sprintf("syncNodeQuotaStatus set disk %s disable start", disabledisk))
		err := r.xfsquotamanager.SetQuotaDiskDisabled(disabledisk, true)
		r.tl.Log(fmt.Sprintf("syncNodeQuotaStatus set disk %s disable stop", disabledisk))
		if err != nil {
			glog.Errorf("set disk %s disable fail:%v", disabledisk, err)
		}
		disabled[path.Clean(disabledisk)] = true
	}
	// set disk enable
	for _, disk := range r.diskInfos {
		if disabled, _ := disabled[path.Clean(disk.MountPath)]; disabled == false {
			r.tl.Log(fmt.Sprintf("syncNodeQuotaStatus set disk %s enable start", disk.MountPath))
			err := r.xfsquotamanager.SetQuotaDiskDisabled(disk.MountPath, false)
			r.tl.Log(fmt.Sprintf("syncNodeQuotaStatus set disk %s enable stop", disk.MountPath))
			if err != nil {
				glog.Errorf("set disk %s enable fail:%v", disk.MountPath, err)
			}
		}
	}

	info := make(xfsquotamanager.NodeDiskQuotaInfoList, 0)

	for _, di := range r.diskInfos {
		info = append(info, xfsquotamanager.NodeDiskQuotaInfo{
			MountPath: di.MountPath,
			Allocable: di.Capacity - di.SaveSize,
			Disabled:  di.Disabled,
		})
	}
	sort.Sort(info)
	buf, _ := json.Marshal(info)
	diskQuotaStr := string(buf)

	quotaStatus := xfsquotamanager.QuotaStatus{
		DiskStatus: make([]xfsquotamanager.DiskQuotaStatus, 0, len(r.diskInfos)),
	}
	funcSizeToStr := func(size int64) string {
		if size < 1024 {
			return fmt.Sprintf("%dB", size)
		} else if size < 1024*1024 {
			return fmt.Sprintf("%.2fKB", float64(size)/1024.0)
		} else if size < 1024*1024*1024 {
			return fmt.Sprintf("%.2fMB", float64(size)/(1024.0*1024.0))
		} else if size < 1024*1024*1024*1024 {
			return fmt.Sprintf("%.2fGB", float64(size)/(1024.0*1024.0*1024.0))
		} else {
			return fmt.Sprintf("%.2fTB", float64(size)/(1024.0*1024.0*1024.0*1024.0))
		}
	}
	statusStr := ""
	for _, diskInfo := range r.diskInfos {
		ds := xfsquotamanager.DiskQuotaStatus{
			Capacity:      diskInfo.Capacity,
			CurUseSize:    diskInfo.UsedSize,
			CurQuotaSize:  diskInfo.QuotaedSize,
			AvaliableSize: fmt.Sprintf("%s, %s", funcSizeToStr(diskInfo.Capacity-diskInfo.UsedSize), funcSizeToStr(diskInfo.Capacity-diskInfo.QuotaedSize)),
			MountPath:     diskInfo.MountPath,
		}
		quotaStatus.Capacity += ds.Capacity
		quotaStatus.CurQuotaSize += ds.CurQuotaSize
		quotaStatus.CurUseSize += ds.CurUseSize
		quotaStatus.DiskStatus = append(quotaStatus.DiskStatus, ds)
		statusStr += fmt.Sprintf("%s %d %d %d\n", ds.MountPath, ds.Capacity, ds.CurUseSize, ds.CurQuotaSize)
	}
	writeFile(path.Join(r.xfsquotamanager.GetRootPath(), common.XFSStatusFileName), statusStr)
	quotaStatus.AvaliableSize = fmt.Sprintf("%s, %s", funcSizeToStr(quotaStatus.Capacity-quotaStatus.CurUseSize),
		funcSizeToStr(quotaStatus.Capacity-quotaStatus.CurQuotaSize))

	statusBuf, _ := json.Marshal(quotaStatus)
	quotaStatusStr := string(statusBuf)

	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	changed := false
	if node.Annotations[common.NodeDiskQuotaInfoAnn] != diskQuotaStr {
		//node.Annotations[xfsquotamanager.NodeDiskQuotaInfoAnn] = diskQuotaStr
		changed = true

	}
	if node.Annotations[common.NodeDiskQuotaStatusAnn] != quotaStatusStr {
		//node.Annotations[xfsquotamanager.NodeDiskQuotaStatusAnn] = quotaStatusStr
		changed = true
	}
	if changed {
		tryUpdate := func(tryTime int) error {
			var lastErr error
			for i := 0; i < tryTime; i++ {
				curNode, errGet := r.k8sClient.GetNodeByName(r.nodeName)
				if errGet != nil {
					return errGet
				}
				if curNode.Annotations == nil {
					curNode.Annotations = make(map[string]string)
				}
				curNode.Annotations[common.NodeDiskQuotaInfoAnn] = diskQuotaStr
				curNode.Annotations[common.NodeDiskQuotaStatusAnn] = quotaStatusStr
				errUpdate := r.k8sClient.UpdateNode(curNode)
				if errUpdate != nil {
					time.Sleep(100 * time.Millisecond)
					lastErr = errUpdate
				} else {
					return nil
				}
			}
			return lastErr
		}
		return tryUpdate(3)
	}
	return nil
}

func (r *SyncWorker) getNodeQuotadiskDisableList(node *v1.Node) []string {
	ret := make([]string, 0, 10)
	if node == nil || node.Annotations == nil || node.Annotations[common.NodeDiskQuotaDisableListAnn] == "" {
		return ret
	}
	list := node.Annotations[common.NodeDiskQuotaDisableListAnn]
	strs := strings.Split(list, ",")
	for _, str := range strs {
		str = strings.Trim(str, " ")
		if str != "" {
			ret = append(ret, str)
		}
	}
	return ret
}

func (r *SyncWorker) syncQuotaPathUsage() error {
	needUpdatePV := make(map[string]MountInfoList)
	for _, disk := range r.diskInfos {
		r.tl.Log(fmt.Sprintf("syncQuotaPathUsage check disk %s start", disk.MountPath))
		for _, info := range disk.PathQuotaInfos {
			if info.IsCSIQuotaPath == false {
				continue
			}
			r.tl.Log(fmt.Sprintf("syncQuotaPathUsage check disk path %s start", info.Path))
			volumeMountInfoList := MountInfoList{}
			if list, exist := needUpdatePV[info.VolumeId]; exist == true && list != nil {
				volumeMountInfoList = list
			}
			if mountPaths, err := r.xfsquotamanager.GetQuotaPathMountPaths(info.Path); err != nil || hasValidMountPoint(mountPaths, r.k8sClient) == false {
				//fmt.Printf("patrick debug %s, err:%v, %t\n", info.Path, err, hasValidMountPoint(mountPaths, r.k8sClient))
			} else {
				if info.IsKeep == true || r.k8sClient.IsPodExistCheckCacheOnly(info.PodId) == true { // not keep quota dir should be recycled when pod is deleted
					volumeMountInfoList = append(volumeMountInfoList, MountInfo{
						HostPath:             info.Path,
						VolumeQuotaSize:      info.HardQuota,
						VolumeCurrentSize:    info.UsedSize,
						VolumeCurrentFileNum: 0,
						PodInfo:              r.getPodInfo(info.OwnerId, info.PodId),
					})
				} else {
					fmt.Printf("quotapath %s iskeep:%t, podId:%s not added\n", info.Path, info.IsKeep, info.PodId)
				}
			}
			r.tl.Log(fmt.Sprintf("syncQuotaPathUsage check disk path %s stop", info.Path))
			needUpdatePV[info.VolumeId] = volumeMountInfoList
		}
		r.tl.Log(fmt.Sprintf("syncQuotaPathUsage check disk %s stop", disk.MountPath))
	}
	r.tl.Log(fmt.Sprintf("syncQuotaPathUsage ListCSIPV start"))
	csiPVs, err1 := r.k8sClient.ListCSIPV(r.driverName)
	r.tl.Log(fmt.Sprintf("syncQuotaPathUsage ListCSIPV stop"))
	if err1 != nil {
		return fmt.Errorf("ListCSIPV driver %s err:%v", r.driverName, err1)
	}

	tryUpdate := func(tryTime int, pvName string, mountList MountInfoList) error {
		var lastErr error
		for i := 0; i < tryTime; i++ {
			curPV, errGet := r.k8sClient.GetPVByName(pvName)
			if errGet != nil {
				return errGet
			}
			if changed, err := setPVMountInfo(curPV, r.nodeName, mountList); err != nil {
				return err
			} else if changed == false {
				return nil
			}
			errUpdate := r.k8sClient.UpdatePV(curPV)
			if errUpdate != nil {
				time.Sleep(100 * time.Millisecond)
				lastErr = errUpdate
			}
			return nil
		}
		return lastErr
	}
	for _, pv := range csiPVs {
		r.tl.Log(fmt.Sprintf("syncQuotaPathUsage update pv %s start", pv.Name))
		if mountList, exist := needUpdatePV[getPVVolumeId(pv)]; exist == true {
			errUpdate := tryUpdate(3, pv.Name, mountList)
			if errUpdate != nil {
				glog.Errorf("syncQuotaPathUsage UpdatePV pv:%s nodeName:%s err:%v", pv.Name, r.nodeName, errUpdate)
			}
		}
		r.tl.Log(fmt.Sprintf("syncQuotaPathUsage update pv %s stop", pv.Name))
	}
	return nil
}

func (r *SyncWorker) getPodInfo(ownerId, podId string) *PodInfo {
	if ownerId == "" && podId == "" {
		return nil
	}
	if ownerId != "" {
		ns, name := r.k8sClient.GetPodNsAndNameByUID(ownerId)
		if ns != "" && name != "" {
			return &PodInfo{Info: fmt.Sprintf("%s:%s:%s", ns, name, ownerId)}
		}
		return nil
	}
	if podId != "" {
		ns, name := r.k8sClient.GetPodNsAndNameByUID(podId)
		if ns != "" && name != "" {
			return &PodInfo{Info: fmt.Sprintf("%s:%s:%s", ns, name, podId)}
		}
		return nil
	}
	return nil
}
func (r *SyncWorker) cleanOrphanQuotaPath() error {
	csiPVs, err1 := r.k8sClient.ListCSIPV(r.driverName)
	if err1 != nil {
		return fmt.Errorf("ListCSIPV driver %s err:%v", r.driverName, err1)
	}
	r.tl.Log("cleanOrphanQuotaPath start ListHostPathPV")
	hostpathPVs, err2 := r.k8sClient.ListHostPathPV()
	if err2 != nil {
		return fmt.Errorf("ListHostPathPV err:%v", err2)
	}
	activePaths := make(map[string]bool)
	for _, pv := range csiPVs {
		if paths, err := getPVQuotaPaths(pv.Annotations, r.nodeName); err != nil {
			return fmt.Errorf("getPVQuotaPaths from csi pv:%s err:%v", pv.Name, err)
		} else {
			for _, p := range paths {
				activePaths[p] = true
			}
		}
	}
	for _, pv := range hostpathPVs {
		if paths, err := getPVQuotaPaths(pv.Annotations, r.nodeName); err != nil {
			return fmt.Errorf("getPVQuotaPaths from hostpath pv:%s err:%v", pv.Name, err)
		} else {
			for _, p := range paths {
				activePaths[p] = true
			}
		}
	}
	nextLoopShouldDeletePathMap := make(map[string]bool)
	for _, disk := range r.diskInfos {
		r.tl.Log(fmt.Sprintf("cleanOrphanQuotaPath disk start %s", disk.MountPath))
		for _, dir := range getSubDirs(disk.MountPath) {
			if r.xfsquotamanager.IsCSIQuotaDir(dir) == false {
				continue
			}
			r.tl.Log(fmt.Sprintf("cleanOrphanQuotaPath quotaPath %s start", dir))
			if r.isQuotPathUseInterface.IsQuotaPathUsed(xfsquotamanager.GetQuotaVolumePath(dir)) == true {
				if _, exist := activePaths[path.Clean(dir)]; exist == false {
					quotaVolume := xfsquotamanager.GetQuotaVolumePath(dir)
					ownerId := r.xfsquotamanager.GetQuotaPathOwnerId(dir)
					if len(ownerId) == 0 { // pv is share mode
						mountDirs, errGet := r.xfsquotamanager.GetQuotaPathMountPaths(dir)
						if errGet != nil {
							glog.Errorf("GetQuotaPathMountPaths %s err:%v", dir, errGet)
							continue
						}
						isUsing := false
						for _, md := range mountDirs {
							podIdFromMountPath := getPodIdFromMountTargetPath(md)
							if podIdFromMountPath == "" {
								glog.Errorf("getPodIdFromMountTargetPath %s err", md)
								continue
							}
							if r.k8sClient.IsPodExistCheckCacheOnly(podIdFromMountPath) {
								glog.Infof("quota path %s is used and owner is %s\n", dir, podIdFromMountPath)
								isUsing = true
								break
							}
						}
						if isUsing == false {
							err := r.isQuotPathUseInterface.DeleteQuotaPathUsed(quotaVolume, "")
							glog.Infof("quota path %s is used and owner is %s, umount err:%v\n", dir, ownerId, err)
							if err != nil {
								continue
							}
						}
					} else {
						err := r.isQuotPathUseInterface.DeleteQuotaPathUsed(quotaVolume, ownerId)
						glog.Infof("quota path %s is used and owner is %s, umount err:%v\n", dir, ownerId, err)
						if err != nil {
							continue
						}
					}
				} else {
					continue
				}
			}
			if _, exist := activePaths[path.Clean(dir)]; exist == false {
				nextLoopShouldDeletePathMap[dir] = true
			}
			r.tl.Log(fmt.Sprintf("cleanOrphanQuotaPath quotaPath %s stop", dir))
		}
		r.tl.Log(fmt.Sprintf("cleanOrphanQuotaPath disk stop %s", disk.MountPath))
	}
	for deletePath := range r.shouldDeleteQuotaPaths {
		if _, exist := nextLoopShouldDeletePathMap[deletePath]; exist == true { // double check the quota path should be deleted
			r.tl.Log(fmt.Sprintf("cleanOrphanQuotaPath DeleteQuotaByPath %s start", deletePath))
			ok, err := r.xfsquotamanager.DeleteQuotaByPath(deletePath)
			r.tl.Log(fmt.Sprintf("cleanOrphanQuotaPath DeleteQuotaByPath %s stop", deletePath))
			glog.Infof("DeleteQuotaByPath %s , ok:%t, err:%v", deletePath, ok, err)
		}
	}
	r.shouldDeleteQuotaPaths = nextLoopShouldDeletePathMap
	return nil
}

func getSubDirs(parentDir string) []string {
	parentDir = path.Clean(parentDir)
	subDirs := make([]string, 0)
	dir, err := ioutil.ReadDir(parentDir)
	if err != nil {
		return subDirs
	}
	for _, fi := range dir {
		if fi.IsDir() {
			subDirs = append(subDirs, path.Clean(parentDir+string(os.PathSeparator)+fi.Name()))
		}
	}
	return subDirs
}

func getPVVolumeId(pv *v1.PersistentVolume) string {
	if pv.Spec.CSI == nil {
		return ""
	}
	return pv.Spec.CSI.VolumeHandle
}

func setPVMountInfo(pv *v1.PersistentVolume, nodeName string, list MountInfoList) (bool, error) {
	if pv.Annotations == nil {
		pv.Annotations = make(map[string]string)
	}
	sort.Sort(list)
	hostPathPVMountInfoList, err := getPVHostPathPVMountInfoList(pv.Annotations)
	if err != nil {
		return false, err
	}
	find := false
	for i, node := range hostPathPVMountInfoList {
		if node.NodeName == nodeName {
			if isKeep(map[string]string{}, pv) == true {
				hostPathPVMountInfoList[i].MountInfos = mergeMountInfoList(hostPathPVMountInfoList[i].MountInfos, list)
			} else {
				hostPathPVMountInfoList[i].MountInfos = list
			}
			if len(hostPathPVMountInfoList[i].MountInfos) == 0 {
				hostPathPVMountInfoList = append(hostPathPVMountInfoList[0:i], hostPathPVMountInfoList[i+1:]...)
			}
			find = true
			break
		}
	}
	if find == false && len(list) > 0 {
		hostPathPVMountInfoList = append(hostPathPVMountInfoList, HostPathPVMountInfo{
			NodeName:   nodeName,
			MountInfos: list,
		})
		sort.Sort(hostPathPVMountInfoList)
	}
	buf, errMarshal := json.Marshal(hostPathPVMountInfoList)
	if errMarshal != nil {
		return false, fmt.Errorf("Marshal list err:%v", errMarshal)
	}
	oldStr := pv.Annotations[common.PVVolumeHostPathMountNode]
	pv.Annotations[common.PVVolumeHostPathMountNode] = string(buf)
	return oldStr != string(buf), nil
}

func mergeMountInfoList(oldList, newList MountInfoList) MountInfoList {
	ret := make(MountInfoList, 0, len(oldList)+len(newList))
	newSet := make(map[string]bool)
	for _, info := range newList {
		newSet[path.Clean(info.HostPath)] = true
		ret = append(ret, info)
	}
	for _, info := range oldList {
		if newSet[path.Clean(info.HostPath)] == false {
			ret = append(ret, info)
		}
	}
	sort.Sort(ret)
	return ret
}

func getPVHostPathPVMountInfoList(annotation map[string]string) (HostPathPVMountInfoList, error) {
	if annotation == nil || annotation[common.PVVolumeHostPathMountNode] == "" {
		return HostPathPVMountInfoList{}, nil
	}
	infoStr := annotation[common.PVVolumeHostPathMountNode]
	hostPathPVMountInfoList := HostPathPVMountInfoList{}
	err := json.Unmarshal([]byte(infoStr), &hostPathPVMountInfoList)
	if err != nil {
		return HostPathPVMountInfoList{}, fmt.Errorf("Unmarshal %s err:%v", infoStr, err)
	}
	return hostPathPVMountInfoList, nil
}

func getPVQuotaPaths(annotation map[string]string, nodeName string) ([]string, error) {
	hostPathPVMountInfoList, err := getPVHostPathPVMountInfoList(annotation)
	if err != nil {
		return []string{}, err
	}

	for _, item := range hostPathPVMountInfoList {
		if item.NodeName == nodeName {
			ret := make([]string, 0, len(item.MountInfos))
			for _, info := range item.MountInfos {
				ret = append(ret, path.Clean(info.HostPath))
			}
			return ret, nil
		}
	}
	return []string{}, nil
}

func isCSIVolumeId(volumeId string) bool {
	return strings.Contains(volumeId, "csi")
}

func isPathExist(dir string) bool {
	var exist = true
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

var mounter mount.Interface

func hasValidMountPoint(mountPaths []string, k8sClient k8sInterface) bool {
	if mounter == nil {
		mounter = mount.New("")
	}
	for _, mp := range mountPaths {
		if notMount, err := mounter.IsLikelyNotMountPoint(mp); notMount == false && err == nil {
			podId := getPodIdFromMountTargetPath(mp)
			if k8sClient.IsPodExistCheckCacheOnly(podId) == true {
				return true
			}
		}
	}
	return false
}

func writeFile(filepath, content string) error {
	baseDir := path.Dir(filepath)
	if isPathExist(baseDir) == false {
		if errMake := os.MkdirAll(baseDir, 755); errMake != nil {
			return fmt.Errorf("mkdir %s err:%v", baseDir, errMake)
		}
	}
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil || file == nil {
		return fmt.Errorf("file %s open error %v\n", filepath, err)
	}
	defer file.Close()
	_, e := io.WriteString(file, content)
	if e != nil {
		return fmt.Errorf("file %s write error %v\n", filepath, e)
	}
	return nil
}

type PVScaleState struct {
	NodePath     string `json:"nodePath"`
	ToCapacity   int64  `json:"toCapacity"`
	ScaleSuccess bool   `json:"scaleSuccess"`
	Error        string `json:"error"`
}

type PVScaleStates map[string]map[string]PVScaleState

func GetPVScaleStates(pv *v1.PersistentVolume) PVScaleStates {
	ret := make(PVScaleStates)
	if pv.Annotations != nil && pv.Annotations[common.PVHostPathScaleStateAnn] != "" {
		json.Unmarshal([]byte(pv.Annotations[common.PVHostPathScaleStateAnn]), &ret)
	}
	return ret
}

func GetNodePathSyncState(states PVScaleStates, nodeName, path string, toCapacity int64) (exist bool, errStr string) {
	if nodeState, found := states[nodeName]; found == false {
		return false, ""
	} else {
		if s, found := nodeState["*"]; found && s.ToCapacity == toCapacity {
			return true, ""
		} else if s, found := nodeState[getNodePath(nodeName, path)]; found && s.ToCapacity == toCapacity {
			return true, s.Error
		} else {
			return false, ""
		}
	}
}

func SetPVScaleStates(pv *v1.PersistentVolume, stats PVScaleStates) bool {
	changed := false
	if pv.Annotations == nil {
		pv.Annotations = make(map[string]string)
		changed = true
	}
	buf, _ := json.Marshal(stats)
	if string(buf) != pv.Annotations[common.PVHostPathScaleStateAnn] {
		pv.Annotations[common.PVHostPathScaleStateAnn] = string(buf)
		changed = true
	}
	return changed
}

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
