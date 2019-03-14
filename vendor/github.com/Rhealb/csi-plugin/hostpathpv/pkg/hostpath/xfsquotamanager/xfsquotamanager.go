package xfsquotamanager

import (
	"fmt"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager/common"
	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager/prjquota"

	glog "k8s.io/klog"
)

type Interface interface {
	Init() error
	GetQuotaDiskNum() int
	IsQuotaDiskDisabled(diskpath string) bool
	GetQuotaDiskInfos() []DiskQuotaInfo
	GetQuotaPathOwnerId(quotaPath string) string
	GetQuotaInfoByPath(path string) (exists bool, info PathQuotaInfo)
	GetQuotaInfoByVolumePodId(volumeId, podId string) (exists bool, info PathQuotaInfo)
	GetQuotaInfosByVolumeId(volumeId string) []PathQuotaInfo
	SetQuotaDiskDisabled(diskpath string, disabled bool) error
	AddQuotaPath(volumeId, podId string, softQuota, hardQuota int64, recycle bool) (ok bool, volumePath string, err error)
	ChangeQuotaPathQuota(projectId int64, softQuota, hardQuota int64) (ok bool, err error)
	DeleteQuotaByPath(path string) (ok bool, err error)
	GetQuotaPathMountPaths(quotaPath string) ([]string, error)
	AddQuotaPathMountPath(quotaPath, mountPath string) error
	RemoveQuotaPathMountPath(quotaPath, mountPath string) error
	RemoveMountPath(mountPath string) error
	IsCSIQuotaDir(dir string) bool
	GetRootPath() string
	IsQuotaPathExistAndUsed(quotaPath string) (exist, used bool, err error)
}

type QuotaDiskSortItem struct {
	diskMountPath string
	freeQuotaSize int64
	isConflict    bool
}
type QuotaDiskSortItemList []QuotaDiskSortItem

func (qdsil QuotaDiskSortItemList) Len() int { return len(qdsil) }
func (qdsil QuotaDiskSortItemList) Less(i, j int) bool {
	if qdsil[i].isConflict != qdsil[j].isConflict {
		return qdsil[i].isConflict == false
	}
	return qdsil[i].freeQuotaSize > qdsil[j].freeQuotaSize
}
func (qdsil QuotaDiskSortItemList) Swap(i, j int) {
	qdsil[i], qdsil[j] = qdsil[j], qdsil[i]
}

type NodeDiskQuotaInfo struct {
	MountPath string
	Allocable int64
	Disabled  bool
}

type NodeDiskQuotaInfoList []NodeDiskQuotaInfo

func (l NodeDiskQuotaInfoList) Len() int { return len(l) }
func (l NodeDiskQuotaInfoList) Less(i, j int) bool {
	return l[i].MountPath < l[j].MountPath
}
func (l NodeDiskQuotaInfoList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

type DiskQuotaStatus struct {
	Capacity      int64
	CurUseSize    int64
	CurQuotaSize  int64
	AvaliableSize string
	MountPath     string
}

type QuotaStatus struct {
	Capacity      int64
	CurUseSize    int64
	CurQuotaSize  int64
	AvaliableSize string
	Disabled      bool
	DiskStatus    []DiskQuotaStatus
}

type PathQuotaInfo struct {
	Path           string
	VolumeId       string
	PodId          string
	OwnerId        string
	ProjectId      int64
	ProjectName    string
	UsedSize       int64
	SoftQuota      int64
	HardQuota      int64
	IsKeep         bool
	IsShare        bool
	IsCSIQuotaPath bool
}

func GetVolumePath(host prjquota.HostInterface, quotaPath string) string {
	quotaPath = path.Clean(quotaPath)
	ret := path.Join(quotaPath, common.XfsKeepForOnePodInnerDir)
	if host.IsPathExist(ret) == false {
		if err := host.MkDir(ret); err != nil {
			return ""
		}
	}

	if err := host.WriteFile(path.Join(quotaPath, common.XfsCSIFlagFile), "true"); err != nil {
		parentDir := path.Dir(quotaPath)
		host.WriteFile(path.Join(parentDir, common.XfsCSISaveDirName, path.Base(quotaPath)), "true")
	}
	return ret
}

func IsPathCSIQuotaPath(host prjquota.HostInterface, quotaPath string) bool {
	quotaPath = path.Clean(quotaPath)
	parentDir := path.Dir(quotaPath)
	if host.ReadFile(path.Join(quotaPath, common.XfsCSIFlagFile)) != "" {
		return true
	}

	if host.ReadFile(path.Join(parentDir, common.XfsCSISaveDirName, path.Base(quotaPath))) != "" {
		return true
	}
	return false
}

func removeQuotaPathCSIFlagFile(host prjquota.HostInterface, quotaPath string) error {
	quotaPath = path.Clean(quotaPath)
	parentDir := path.Dir(quotaPath)
	flagPath := path.Join(parentDir, common.XfsCSISaveDirName, path.Base(quotaPath))
	if host.IsPathExist(flagPath) == true {
		return host.DeleteFile(flagPath)
	}
	return nil
}

type DiskQuotaInfo struct {
	Capacity       int64
	UsedSize       int64
	AvailSize      int64
	SaveSize       int64
	QuotaedSize    int64
	MountPath      string
	Disabled       bool
	PathQuotaInfos []PathQuotaInfo
}

type k8sInterface interface {
	IsPodExist(podId string) bool
	GetParentId(podId string) string
}

func GetQuotaVolumePath(quotaPath string) string {
	return path.Join(quotaPath, common.XfsKeepForOnePodInnerDir)
}

func getQuotaPathMountInfoFilePath(quotaPath string) string {
	quotaPath = path.Clean(quotaPath)
	if path.Base(quotaPath) == common.XfsKeepForOnePodInnerDir {
		quotaPath = path.Dir(quotaPath)
	}
	return path.Join(quotaPath, common.XfsMountedToPathsFile)
}

type XFSQuotaManager struct {
	prjquota       prjquota.Interface
	host           prjquota.HostInterface
	quotaRootPath  string
	mu             sync.Mutex
	diskQuotaInfos []DiskQuotaInfo
	k8sInterface   k8sInterface
}

func NewFakeXFSQuotaManager(quotaRootPath string, host prjquota.HostInterface, xfsStateInfo []prjquota.XFSStateInfo,
	disksizemap map[string]int64, pathQuotaInfos []prjquota.FakePathQuotaInfo, k8sInterface k8sInterface) Interface {
	ret := &XFSQuotaManager{
		host:           host,
		quotaRootPath:  quotaRootPath,
		prjquota:       prjquota.NewFake(host, xfsStateInfo, disksizemap, pathQuotaInfos),
		k8sInterface:   k8sInterface,
		diskQuotaInfos: make([]DiskQuotaInfo, 0, 10),
	}
	if err := ret.Init(); err != nil {
		glog.Errorf("NewXFSQuotaManager init error :%v", err)
		return &NoneQuotaManager{}
	}

	return ret
}

func NewXFSQuotaManager(quotaRootPath string, k8sInterface k8sInterface) Interface {
	host := prjquota.NewHostRunner()
	ret := &XFSQuotaManager{
		host:           host,
		quotaRootPath:  quotaRootPath,
		prjquota:       prjquota.New(host, quotaRootPath),
		k8sInterface:   k8sInterface,
		diskQuotaInfos: make([]DiskQuotaInfo, 0, 10),
	}
	if err := ret.Init(); err != nil {
		glog.Errorf("NewXFSQuotaManager init error :%v", err)
		return &NoneQuotaManager{}
	}

	return ret
}

func (xfsqm *XFSQuotaManager) GetRootPath() string {
	return xfsqm.quotaRootPath
}

func (xfsqm *XFSQuotaManager) Init() error {
	ok, err := checkSystemSupportXFSQuota()
	if ok != true || err != nil {
		return fmt.Errorf("Init checkSystemSupportXFSQuota error=%v", err)
	}
	if ok, err = xfsqm.prjquota.SystemSupport(); ok != true || err != nil {
		return fmt.Errorf("xfs_quota not support %v", err)
	}
	if ok = xfsqm.host.IsPathExist(xfsqm.quotaRootPath); ok == false {
		return fmt.Errorf("quotaRootPath %s is not exist", xfsqm.quotaRootPath)
	}
	if err = xfsqm.loadQuotaInfo(); err != nil {
		return fmt.Errorf("loadQuotaInfo error =%v", err)
	}
	return nil
}

func (xfsqm *XFSQuotaManager) IsQuotaDiskDisabled(diskpath string) bool {
	return xfsqm.isQuotaDiskDisabled(diskpath, true)
}

func (xfsqm *XFSQuotaManager) GetQuotaDiskNum() int {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	xfsqm.loadQuotaInfo()
	return len(xfsqm.diskQuotaInfos)
}

func (xfsqm *XFSQuotaManager) GetQuotaDiskInfos() []DiskQuotaInfo {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	xfsqm.loadQuotaInfo()
	ret := make([]DiskQuotaInfo, 0, len(xfsqm.diskQuotaInfos))
	for _, disk := range xfsqm.diskQuotaInfos {
		ret = append(ret, disk)
	}
	return ret
}
func (xfsqm *XFSQuotaManager) GetQuotaInfoByPath(quotaPath string) (exists bool, quotaInfo PathQuotaInfo) {
	quotaPath = path.Clean(quotaPath)
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	xfsqm.loadQuotaInfo()
	for _, disk := range xfsqm.diskQuotaInfos {
		for _, info := range disk.PathQuotaInfos {
			if path.Clean(info.Path) == quotaPath {
				return true, info
			}
		}
	}
	return false, PathQuotaInfo{}
}

func (xfsqm *XFSQuotaManager) GetQuotaInfoByVolumePodId(volumeId, podId string) (exists bool, info PathQuotaInfo) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	xfsqm.loadQuotaInfo()
	for _, disk := range xfsqm.diskQuotaInfos {
		for _, info := range disk.PathQuotaInfos {
			if info.VolumeId == volumeId && info.OwnerId == podId {
				return true, info
			}
		}
	}
	return false, PathQuotaInfo{}
}

func (xfsqm *XFSQuotaManager) GetQuotaInfosByVolumeId(volumeId string) []PathQuotaInfo {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	xfsqm.loadQuotaInfo()
	ret := []PathQuotaInfo{}
	for _, disk := range xfsqm.diskQuotaInfos {
		for _, info := range disk.PathQuotaInfos {
			if info.VolumeId == volumeId {
				ret = append(ret, info)
			}
		}
	}
	return ret
}

func (xfsqm *XFSQuotaManager) SetQuotaDiskDisabled(diskpath string, disabled bool) error {
	diskpath = path.Clean(diskpath)
	var find bool
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	xfsqm.loadQuotaInfo()
	for _, disk := range xfsqm.diskQuotaInfos {
		if path.Clean(disk.MountPath) == diskpath {
			find = true
			break
		}
	}
	if find == false {
		return fmt.Errorf("disk %s is not quota disk", diskpath)
	}
	var disablestr string = "no"
	if disabled == true {
		disablestr = "yes"
	}
	return xfsqm.host.WriteFile(path.Join(diskpath, common.XfsDiskDisablefilename), disablestr)
}

func (xfsqm *XFSQuotaManager) IsQuotaPathExistAndUsed(quotaPath string) (exist, used bool, err error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	if err := xfsqm.loadQuotaInfo(); err != nil {
		return false, false, fmt.Errorf("loadQuotaInfo err:%v", err)
	}
	if exist := xfsqm.host.IsPathExist(quotaPath); exist == false {
		return false, false, nil
	}

	isQuotaDiskExist := false
	for _, disk := range xfsqm.diskQuotaInfos {
		if strings.HasPrefix(quotaPath, disk.MountPath) == true {
			isQuotaDiskExist = true
			break
		}
	}
	if isQuotaDiskExist == false {
		return false, false, fmt.Errorf("quota disk is not exist")
	}
	if strings.Contains(quotaPath, common.XfsKeepForOnePodFlag) == false {
		return true, false, fmt.Errorf("%s is not keeptrue quota path", quotaPath)
	}
	curOwnerId := common.GetOwnerId(quotaPath, xfsqm.host.ReadFile)
	if xfsqm.k8sInterface.IsPodExist(curOwnerId) {
		return true, true, nil
	}
	return true, false, nil
}

func (xfsqm *XFSQuotaManager) AddQuotaPath(volumeId, podId string, softQuota, hardQuota int64, recycle bool) (bool, string, error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()

	// step1: call loadQuotaInfo refresh current quota info
	if err := xfsqm.loadQuotaInfo(); err != nil {
		return false, "", fmt.Errorf("loadQuotaInfo err:%v", err)
	}

	// step2: check is added
	for _, disk := range xfsqm.diskQuotaInfos {
		for _, info := range disk.PathQuotaInfos {
			if recycle == true { // if recycle == true the info.PodId maybe not equal podId
				if info.VolumeId == volumeId && info.OwnerId == podId && info.SoftQuota == softQuota && info.HardQuota == hardQuota {
					return true, GetVolumePath(xfsqm.host, info.Path), nil
				}
			} else { // if recycle == false the info.PodId must equal podId
				if info.VolumeId == volumeId && info.PodId == podId && info.SoftQuota == softQuota && info.HardQuota == hardQuota {
					return true, GetVolumePath(xfsqm.host, info.Path), nil
				}
			}
		}
	}

	// step3: get or create a quotapath
	var quotaPath string
	if p, err := xfsqm.getOrCreateQuotaPath(volumeId, podId, recycle, hardQuota); err != nil {
		return false, "", fmt.Errorf("getOrCreateQuotaPath for volume:%s pod:%s, recycle:%t, hardQuota:%d fail:%v", volumeId, podId, recycle, hardQuota, err)
	} else {
		quotaPath = p
	}

	// step4: set quota
	volumePath := GetVolumePath(xfsqm.host, quotaPath)
	if err := xfsqm.setPathQuota(volumePath, softQuota, hardQuota); err != nil {
		return false, "", fmt.Errorf("setPathQuota err:%v", err)
	}

	// step5: set quotapath ownerid
	if recycle == true && podId != "" { // only recycle quotapath need create ownerid
		if err := common.SetOwnerId(quotaPath, podId, xfsqm.host.WriteFile); err != nil {
			return false, "", fmt.Errorf("set quotapath %s owner id err:%v", quotaPath, err)
		}
	}
	return true, volumePath, nil
}

func (xfsqm *XFSQuotaManager) ChangeQuotaPathQuota(projectId int64, softQuota, hardQuota int64) (ok bool, err error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()

	for _, disk := range xfsqm.diskQuotaInfos {
		for _, info := range disk.PathQuotaInfos {
			if info.ProjectId == projectId {
				if info.UsedSize > hardQuota {
					return false, fmt.Errorf("project %d used is %d cannot change quota to %d", projectId, info.UsedSize, hardQuota)
				} else if info.HardQuota < hardQuota && (disk.Capacity-disk.QuotaedSize-disk.SaveSize) < (hardQuota-info.HardQuota) {
					return false, fmt.Errorf("disk %s has not enough quota to set", disk.MountPath)
				}
				if ret, _, _, err := xfsqm.prjquota.LimitQuotaProject(info.ProjectName, softQuota, hardQuota); err != nil {
					return false, fmt.Errorf("LimitQuotaProject %d %d err:%v, ret:%s", softQuota, hardQuota, err, ret)
				} else {
					return true, nil
				}
			}
		}
	}
	return false, fmt.Errorf("project %d is not exist", projectId)
}

func (xfsqm *XFSQuotaManager) DeleteQuotaByPath(quotaPath string) (ok bool, err error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	if err := xfsqm.loadQuotaInfo(); err != nil {
		return false, fmt.Errorf("loadQuotaInfo err:%v", err)
	}
	quotaPath = path.Clean(quotaPath)

	for _, disk := range xfsqm.diskQuotaInfos {
		for _, info := range disk.PathQuotaInfos {
			if path.Clean(info.Path) == quotaPath {
				if info.IsKeep == true {
					if xfsqm.k8sInterface.IsPodExist(info.OwnerId) {
						return false, fmt.Errorf("Quotapath %s is used by %s", quotaPath, info.OwnerId)
					}
				} else {
					if xfsqm.k8sInterface.IsPodExist(info.PodId) {
						return false, fmt.Errorf("Quotapath %s is used by %s", quotaPath, info.PodId)
					}
				}

				if err := xfsqm.prjquota.QuotaDeleteProjectInfo(info.ProjectId); err != nil {
					return false, fmt.Errorf("QuotaDeleteProjectInfo projectid:%d err:%v", info.ProjectId, err)
				}
				if err := xfsqm.host.DeleteFile(info.Path); err != nil {
					return false, fmt.Errorf("remove projectid:%d path %s err:%v", info.ProjectId, info.Path, err)
				}
				if err := xfsqm.host.DeleteFile(common.GetOwinderIdFilePath(info.Path)); err != nil {
					glog.Errorf("delete ownerid file %s err:%v", common.GetOwinderIdFilePath(info.Path), err)
				}

				if err := removeQuotaPathCSIFlagFile(xfsqm.host, info.Path); err != nil {
					glog.Errorf("removeQuotaPathCSIFlagFile of %s err:%v", info.Path, err)
				}
				return true, nil
			}
		}
	}
	if xfsqm.host.IsPathExist(quotaPath) == false {
		return true, fmt.Errorf("quotaPath %s is not exist", quotaPath)
	} else {
		return true, xfsqm.host.DeleteFile(quotaPath)
	}
}

func (xfsqm *XFSQuotaManager) GetQuotaPathMountPaths(quotaPath string) ([]string, error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	return xfsqm.getQuotaPathMountPaths(quotaPath)
}

func (xfsqm *XFSQuotaManager) AddQuotaPathMountPath(quotaPath, mountPath string) error {
	mountPath = path.Clean(mountPath)
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()

	existPaths, err := xfsqm.getQuotaPathMountPaths(quotaPath)
	if err != nil {
		return err
	}

	for _, p := range existPaths {
		if p == mountPath { // is added
			return nil
		}
	}
	existPaths = append(existPaths, mountPath)

	infoPaths := getQuotaPathMountInfoFilePath(quotaPath)
	return xfsqm.host.WriteFile(infoPaths, strings.Join(existPaths, "\n"))
}

func (xfsqm *XFSQuotaManager) RemoveQuotaPathMountPath(quotaPath, mountPath string) error {
	mountPath = path.Clean(mountPath)
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()

	existPaths, err := xfsqm.getQuotaPathMountPaths(quotaPath)
	if err != nil {
		return err
	}

	newPaths := make([]string, 0, len(existPaths))
	for _, p := range existPaths {
		if p != mountPath {
			newPaths = append(newPaths, p)
		}
	}

	infoPaths := getQuotaPathMountInfoFilePath(quotaPath)
	return xfsqm.host.WriteFile(infoPaths, strings.Join(newPaths, "\n"))
}

func (xfsqm *XFSQuotaManager) RemoveMountPath(mountPath string) error {
	diskInfos := xfsqm.GetQuotaDiskInfos()
	for _, disk := range diskInfos {
		for _, info := range disk.PathQuotaInfos {
			xfsqm.RemoveQuotaPathMountPath(info.Path, mountPath)
		}
	}
	return nil
}

func (xfsqm *XFSQuotaManager) IsCSIQuotaDir(dir string) bool {
	return IsPathCSIQuotaPath(xfsqm.host, dir)
}

func (xfsqm *XFSQuotaManager) GetQuotaPathOwnerId(quotaPath string) string {
	_, podId, ownerId, _, _, errDirInfo := common.GetInfoFromDirName(quotaPath, xfsqm.host.ReadFile)
	if errDirInfo != nil {
		return ""
	} else if ownerId != "" {
		return ownerId
	} else {
		return podId
	}
}

func (xfsqm *XFSQuotaManager) getQuotaPathMountPaths(quotaPath string) ([]string, error) {
	if xfsqm.host.IsPathExist(quotaPath) == false {
		return []string{}, fmt.Errorf("quotapath %s is not exist", quotaPath)
	}
	infoPaths := getQuotaPathMountInfoFilePath(quotaPath)
	buf := xfsqm.host.ReadFile(infoPaths)
	if buf == "" {
		return []string{}, nil
	}
	strs := strings.Split(buf, "\n")
	ret := make([]string, 0, len(strs))
	for _, str := range strs {
		if strings.Trim(str, " ") != "" {
			ret = append(ret, str)
		}
	}
	return ret, nil
}

func (xfsqm *XFSQuotaManager) getOrCreateQuotaDir(podId, dirName string, hardQuota int64) (string, error) {
	// check whether the dirName is exist
	for _, disk := range xfsqm.diskQuotaInfos {
		if xfsqm.host.IsPathExist(path.Join(disk.MountPath, dirName)) {
			return path.Join(disk.MountPath, dirName), nil
		}
	}

	availableDisk := make(QuotaDiskSortItemList, 0, len(xfsqm.diskQuotaInfos))
	for _, disk := range xfsqm.diskQuotaInfos {
		if disk.Disabled == true { // this quota disk disabled
			continue
		}
		if disk.Capacity < disk.SaveSize+disk.QuotaedSize+hardQuota { // free quota size is not enough
			continue
		}
		var isConflict bool = false
		if podId != "" {
			parentId := xfsqm.k8sInterface.GetParentId(podId)
			for _, info := range disk.PathQuotaInfos {
				if info.OwnerId == podId || info.PodId == podId {
					isConflict = true
					break
				}
				if parentId != "" {
					oP := xfsqm.k8sInterface.GetParentId(info.OwnerId)
					pP := xfsqm.k8sInterface.GetParentId(info.PodId)
					if oP == parentId || pP == parentId {
						isConflict = true
						break
					}
				}
			}
		}
		availableDisk = append(availableDisk, QuotaDiskSortItem{
			diskMountPath: disk.MountPath,
			freeQuotaSize: disk.Capacity - (disk.SaveSize + disk.QuotaedSize),
			isConflict:    isConflict,
		})
	}

	if len(availableDisk) == 0 {
		return "", fmt.Errorf("has no disk can match the hardquota %d", hardQuota)
	}
	if len(availableDisk) == 1 {
		return path.Join(availableDisk[0].diskMountPath, dirName), nil
	}
	sort.Sort(availableDisk)
	return path.Join(availableDisk[0].diskMountPath, dirName), nil
}

func (xfsqm *XFSQuotaManager) listAllQuotaPaths() []string {
	allExistPaths := make([]string, 0, 100)
	for _, disk := range xfsqm.diskQuotaInfos {
		allExistPaths = append(allExistPaths, xfsqm.host.GetSubDirs(disk.MountPath)...)
	}
	return allExistPaths
}

func (xfsqm *XFSQuotaManager) getOrCreateQuotaPath(volumeId, podId string, recycle bool, hardQuota int64) (string, error) {
	// step1: check whether podId had created a quotapath, the same pod should create the same podid dir
	for _, disk := range xfsqm.diskQuotaInfos {
		//		for _, info := range disk.PathQuotaInfos {
		//			if recycle == true {
		//				if info.OwnerId == podId { // this pod's other volume has added a quotapath then use the same info.PodId
		//					return xfsqm.getOrCreateQuotaDir(info.PodId, common.GetDirName(volumeId, info.PodId, true), hardQuota)
		//				}
		//			} else {
		//				if info.VolumeId == volumeId && info.PodId == podId {
		//					return xfsqm.getOrCreateQuotaDir(podId, common.GetDirName(volumeId, podId, false), hardQuota)
		//				}
		//			}
		//		}
		for _, quotaPath := range xfsqm.host.GetSubDirs(disk.MountPath) {
			if recycle == true {
				if len(podId) > 0 && common.GetOwnerId(quotaPath, xfsqm.host.ReadFile) == podId {
					_, pid, _, _, _, errDirInfo := common.GetInfoFromDirName(quotaPath, xfsqm.host.ReadFile)
					if errDirInfo == nil {
						return xfsqm.getOrCreateQuotaDir(pid, common.GetDirName(volumeId, pid, true), hardQuota)
					}
				}
			} else {
				if strings.Contains(quotaPath, podId) {
					return xfsqm.getOrCreateQuotaDir(podId, common.GetDirName(volumeId, podId, false), hardQuota)
				}
			}
		}
	}
	// step2: if is recycle should check if has one dir which is created by the volumeId and not using now
	if recycle == true {
		existedPaths := xfsqm.listAllQuotaPaths()
		dirFilterPrefix := common.XfsQuotaDirPrifix + "_" + volumeId + "_" + common.XfsKeepForOnePodFlag + "-"
		for _, dir := range existedPaths {
			basename := path.Base(dir)
			if strings.HasPrefix(basename, dirFilterPrefix) { // only care the basename start with dirFilterPrefix
				keepId, keepPodId := common.GetKeepIdFromDirName(basename)
				// if we have PV1_keepid1 PV2_keepid1 and PV1_keepid1 is used then
				// PV2_keepid1 can not be used
				if keepId != "" && xfsqm.isKeepIdUsedByOthers(keepId, podId) == false {
					return xfsqm.getOrCreateQuotaDir(keepPodId, common.GetDirName(volumeId, keepPodId, true), hardQuota)
				}
			}
		}
	}
	// step3: create new quota dir
	return xfsqm.getOrCreateQuotaDir(podId, common.GetDirName(volumeId, podId, recycle), hardQuota)
}

func (xfsqm *XFSQuotaManager) isKeepIdUsedByOthers(keepId, createPodId string) bool {
	for _, disk := range xfsqm.diskQuotaInfos {
		for _, quotaPath := range xfsqm.host.GetSubDirs(disk.MountPath) {
			//for _, info := range disk.PathQuotaInfos {
			if strings.HasSuffix(path.Base(quotaPath), keepId) {
				ownerId := common.GetOwnerId(quotaPath, xfsqm.host.ReadFile)
				if createPodId != ownerId && xfsqm.k8sInterface.IsPodExist(ownerId) {
					return true
				}
			}
			//}
		}
	}
	return false
}

func (xfsqm *XFSQuotaManager) setPathQuota(quotaPath string, softQuota, hardQuota int64) error {
	// step 1 check whether the quotapath is valid
	var ok bool
	var diskPath string
	for _, disk := range xfsqm.diskQuotaInfos {
		if strings.HasPrefix(quotaPath, disk.MountPath) {
			if disk.Disabled == true && xfsqm.host.IsPathExist(quotaPath) == false {
				return fmt.Errorf("quota disk %s is disabled,cann't add quotapath %s", disk.MountPath, quotaPath)
			} else {
				ok = true
				diskPath = disk.MountPath
				break
			}
		}
	}
	if ok == false {
		return fmt.Errorf("no quota disk match quotapath %s", quotaPath)
	}

	// step 2 check the quotapath is exist, if not create
	if xfsqm.host.IsPathExist(quotaPath) == false {
		if errCreate := xfsqm.host.MkDir(quotaPath); errCreate != nil {
			return fmt.Errorf("create quotapath %s err:%v", quotaPath, errCreate)
		}
	}

	// step 3 get a valid projectid
	projects := xfsqm.prjquota.QuotaProjects()
	usedProjectIdMap := make(map[int64]bool)
	var isQuotaPathAdded bool
	var addedProjectid int64
	quotaPath = path.Clean(quotaPath)
	for _, project := range projects {
		if path.Clean(string(project.Path)) == quotaPath {
			isQuotaPathAdded = true
			addedProjectid = int64(project.ProjectId)
			break
		}
		if int64(project.ProjectId) >= common.XfsQuotaProjIdStart {
			usedProjectIdMap[int64(project.ProjectId)] = true
		}
	}
	nextProjectId := int64(-1)
	nextProjectName := ""
	if isQuotaPathAdded == true {
		nextProjectId = addedProjectid
		projectIds := xfsqm.prjquota.QuotaProjids()
		nextProjectName = string(projectIds.GetProjNameById(prjquota.XFSProjectID(nextProjectId)))
		if nextProjectId < 0 || nextProjectName == "" {
			return fmt.Errorf("project is added but projectid:[%d] or projectname:[%s] is invalid", nextProjectId, nextProjectName)
		}
	} else {
		for i := common.XfsQuotaProjIdStart; i < common.XfsQuotaProjIdStart+common.XfsQuotaMaxProjIdNum; i++ {
			if _, exist := usedProjectIdMap[i]; exist == false {
				nextProjectId = i
				break
			}
		}
		if nextProjectId < common.XfsQuotaProjIdStart || nextProjectId >= common.XfsQuotaProjIdStart+common.XfsQuotaMaxProjIdNum {
			return fmt.Errorf("all projectid [%d-%d] is used", common.XfsQuotaProjIdStart, common.XfsQuotaProjIdStart+common.XfsQuotaMaxProjIdNum-1)
		}
		nextProjectName = fmt.Sprintf("%s%d", common.XfsQuotaProjectPrifix, nextProjectId)
	}

	// step 4 set quota
	if err := xfsqm.prjquota.QuotaAddProjectInfo(nextProjectId, nextProjectName, quotaPath); err != nil {
		return fmt.Errorf("QuotaAddProjectInfo err:%v", err)
	}
	if ret, err := xfsqm.prjquota.QuotaSetupProject(nextProjectName, []string{diskPath}); err != nil {
		return fmt.Errorf("QuotaSetupProject err:%v, ret:%s", err, ret)
	}
	if ret, _, _, err := xfsqm.prjquota.LimitQuotaProject(nextProjectName, softQuota, hardQuota); err != nil {
		return fmt.Errorf("LimitQuotaProject %d %d err:%v, ret:%s", softQuota, hardQuota, err, ret)
	}
	return nil
}

func (xfsqm *XFSQuotaManager) isQuotaDiskDisabled(diskpath string, check bool) bool {
	diskpath = path.Clean(diskpath)
	if check {
		var find bool
		for _, disk := range xfsqm.diskQuotaInfos {
			if path.Clean(disk.MountPath) == diskpath {
				find = true
				break
			}
		}
		if find == false { // not find disk is disabled
			return true
		}
	}
	disable := xfsqm.host.ReadFile(path.Join(diskpath, common.XfsDiskDisablefilename))
	if disable == "yes" {
		return true
	}
	return false
}

func (xfsqm *XFSQuotaManager) loadQuotaInfo() error {
	xfsqm.diskQuotaInfos = make([]DiskQuotaInfo, 0, len(xfsqm.diskQuotaInfos))
	projects := xfsqm.prjquota.QuotaProjects()
	projids := xfsqm.prjquota.QuotaProjids()
	states, errState := xfsqm.prjquota.QuotaState()
	if errState != nil {
		return fmt.Errorf("loadQuotaInfo QuotaState err:%v", errState)
	}
	quotaDiskPaths := make([]string, 0, len(states))
	filtedStates := make([]prjquota.XFSStateInfo, 0, len(states))
	for _, state := range states {
		if state.ProjectQuotaEnforcementOn == true && state.ProjectQuotaAccountingOn == true &&
			strings.HasPrefix(string(state.MountPath), xfsqm.quotaRootPath) {
			quotaDiskPaths = append(quotaDiskPaths, string(state.MountPath))
			filtedStates = append(filtedStates, state)
		}
	}

	dfInfos, errDf := xfsqm.prjquota.QuotaDfInfo(quotaDiskPaths, xfsqm.quotaRootPath)
	if errDf != nil {
		return fmt.Errorf("loadQuotaInfo QuotaDfInfo err:%v", errDf)
	}

	dfInfoMap := make(map[string]prjquota.XFSDfInfo)
	for _, info := range dfInfos {
		dfInfoMap[path.Clean(string(info.MountPath))] = info
	}

	getDfInfo := func(dir string) (used, quota int64, err error) {
		ret, exist := dfInfoMap[path.Clean(dir)]
		if exist == true {
			return ret.Used * 1024, ret.Size * 1024, nil
		}
		return 0, 0, fmt.Errorf("cannot get %s df info", dir)
	}

	getPathQuotaInfos := func(mountDir string) ([]PathQuotaInfo, int64) {
		ret := []PathQuotaInfo{}
		var sumQuota int64
		for _, prj := range projects {
			tmpPath := path.Clean(string(prj.Path))
			if path.Base(tmpPath) == common.XfsKeepForOnePodInnerDir {
				tmpPath = path.Dir(tmpPath)
			}
			if strings.HasPrefix(string(prj.Path), mountDir) && strings.HasPrefix(common.GetBaseNameFromPath(tmpPath), common.XfsQuotaDirPrifix) {
				projectName := string(projids.GetProjNameById(prj.ProjectId))
				volumeId, podId, ownerId, isKeep, isShare, errDirInfo := common.GetInfoFromDirName(tmpPath, xfsqm.host.ReadFile)
				used, _, errGetDf := getDfInfo(string(prj.Path))
				if errDirInfo != nil || errGetDf != nil {
					//glog.Errorf("loadQuotaInfo %s errDirInfo:%v, errGetDf:%v", prj.Path, errDirInfo, errGetDf)
					continue
				}
				_, quota, _ := getDfInfo(path.Clean(string(prj.Path)))
				if prjName := projids.GetProjNameById(prj.ProjectId); prjName != "" {
					ret = append(ret, PathQuotaInfo{
						Path:           tmpPath,
						VolumeId:       volumeId,
						PodId:          podId,
						OwnerId:        ownerId,
						ProjectId:      int64(prj.ProjectId),
						ProjectName:    projectName,
						UsedSize:       used,
						SoftQuota:      quota,
						HardQuota:      quota,
						IsKeep:         isKeep,
						IsShare:        isShare,
						IsCSIQuotaPath: IsPathCSIQuotaPath(xfsqm.host, tmpPath),
					})
					sumQuota += quota
				}
			}
		}
		return ret, sumQuota
	}

	addedDisk := make(map[string]bool)
	for _, state := range filtedStates {
		if diskUsed, diskCapacity, errGetDf := getDfInfo(string(state.MountPath)); errGetDf == nil {
			quotaInfos, sumQuota := getPathQuotaInfos(string(state.MountPath))
			xfsqm.diskQuotaInfos = append(xfsqm.diskQuotaInfos, DiskQuotaInfo{
				Capacity:       diskCapacity,
				UsedSize:       diskUsed,
				AvailSize:      diskCapacity - diskUsed,
				SaveSize:       common.XfsDefaultSaveSize,
				QuotaedSize:    sumQuota,
				MountPath:      string(state.MountPath),
				Disabled:       xfsqm.isQuotaDiskDisabled(string(state.MountPath), false),
				PathQuotaInfos: quotaInfos,
			})
			addedDisk[string(state.MountPath)] = true
		} else {
			glog.Errorf("loadQuotaInfo getDfInfo err:%v", errGetDf)
		}
	}
	return nil
}

func checkSystemSupportXFSQuota() (bool, error) {
	if runtime.GOOS == "windows" { // windows not support the feature
		return false, fmt.Errorf("windows not support disk quota")
	}
	return true, nil
}

// NoneQuotaManager
type NoneQuotaManager struct {
}

func (nqm *NoneQuotaManager) Init() error {
	return nil
}

func (nqm *NoneQuotaManager) GetRootPath() string {
	return ""
}

func (nqm *NoneQuotaManager) IsQuotaDiskDisabled(diskpath string) bool {
	return true
}
func (nqm *NoneQuotaManager) GetQuotaDiskNum() int {
	return 0
}
func (nqm *NoneQuotaManager) GetQuotaDiskInfos() []DiskQuotaInfo {
	return []DiskQuotaInfo{}
}
func (nqm *NoneQuotaManager) GetQuotaInfoByPath(path string) (exists bool, info PathQuotaInfo) {
	return false, PathQuotaInfo{}
}
func (nqm *NoneQuotaManager) GetQuotaInfosByVolumeId(volumeId string) []PathQuotaInfo {
	return []PathQuotaInfo{}
}
func (nqm *NoneQuotaManager) GetQuotaInfoByVolumePodId(volumeId, podId string) (exists bool, info PathQuotaInfo) {
	return false, PathQuotaInfo{}
}
func (nqm *NoneQuotaManager) SetQuotaDiskDisabled(diskpath string, disabled bool) error {
	return nil
}
func (nqm *NoneQuotaManager) AddQuotaPath(volumeId, podId string, softQuota, hardQuota int64, recycle bool) (ok bool, volumePath string, err error) {
	return false, "", nil
}
func (nqm *NoneQuotaManager) DeleteQuotaByPath(path string) (ok bool, err error) {
	return true, nil
}
func (nqm *NoneQuotaManager) GetQuotaPathMountPaths(quotaPath string) ([]string, error) {
	return []string{}, nil
}
func (nqm *NoneQuotaManager) AddQuotaPathMountPath(quotaPath, mountPath string) error {
	return nil
}
func (nqm *NoneQuotaManager) RemoveQuotaPathMountPath(quotaPath, mountPath string) error {
	return nil
}
func (nqm *NoneQuotaManager) RemoveMountPath(mountPath string) error {
	return nil
}
func (nqm *NoneQuotaManager) IsCSIQuotaDir(dir string) bool {
	return false
}
func (nqm *NoneQuotaManager) IsQuotaPathExistAndUsed(quotaPath string) (exist, used bool, err error) {
	return false, false, nil
}
func (nqm *NoneQuotaManager) ChangeQuotaPathQuota(projectId int64, softQuota, hardQuota int64) (ok bool, err error) {
	return false, nil
}
func (nqm *NoneQuotaManager) GetQuotaPathOwnerId(quotaPath string) string {
	return ""
}
