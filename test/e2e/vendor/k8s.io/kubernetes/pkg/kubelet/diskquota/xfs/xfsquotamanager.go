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

package xfs

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/util/sets"
	utilequota "k8s.io/kubernetes/pkg/kubelet/diskquota/xfs/utils"
	utilexec "k8s.io/utils/exec"
)

const (
	xfsQuotaProjectPrifix     string = "k8spro"
	xfsQuotaProjIdStart       int64  = 0
	xfsQuotaDirPrifix         string = "k8squota"
	xfsDefaultSaveSize        int64  = 0
	xfsHistroyNextIdName      string = "nextid"
	xfsKeepForOnePodFlag      string = "kfp"
	xfsKeepForOnePodInnerDir  string = "volume"
	xfsKeepForOnePodSubIdFile string = "curowner"

	xfsOwnerIDSaveDirName  string = "ownerids"
	xfsCSISaveDirName      string = "csis"
	xfsCSIFlagFile         string = "csi"
	xfsStatusFileName      string = "status"
	xfsDiskDisablefilename string = "disable"

	// record the node which have pod mount the pv
	PVCVolumeHostPathMountNode = "io.enndata.kubelet/alpha-pvchostpathnode"

	PVHostPathMountPolicyAnn = "io.enndata.user/alpha-pvhostpathmountpolicy"

	// only for io.enndata.user/alpha-pvhostpathmountpolicy=keep
	PVHostPathMountTimeoutAnn   = "io.enndata.user/alpha-pvhostpathmounttimeout"
	PVHostPathTimeoutDelPodAnn  = "io.enndata.user/alpha-pvhostpathtimeoutdeletepod"
	NodeDiskQuotaInfoAnn        = "io.enndata.kubelet/alpha-nodediskquotainfo"
	NodeDiskQuotaStatusAnn      = "io.enndata.kubelet/alpha-nodediskquotastate"
	NodeDiskQuotaDisableListAnn = "io.enndata.kubelet/alpha-nodediskquotadisablelist"

	PVHostPathCapacityAnn      = "io.enndata.user/alpha-pvhostpathcapcity"
	PVHostPathCapacityStateAnn = "io.enndata.kubelet/alpha-pvhostpathcapcitystate"
	PVHostPathQuotaForOnePod   = "io.enndata.user/alpha-pvhostpathquotaforonepod"
	PVHostPathKeep             = "keep"
	PVHostPathNone             = "none"
)

type QuotaStatusEvent uint

const (
	StatusChange = iota
)

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

type ActiveFun func(id, pvid string) bool

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

type QuotaProjectGenerator struct {
	nextid      int64
	prifix      string
	mu          sync.Mutex
	historyInfo map[string]int64
}

func newQuotaProjectGenerator(startid int64, prifix string, history map[string]int64) *QuotaProjectGenerator {
	return &QuotaProjectGenerator{
		nextid:      startid,
		prifix:      prifix,
		historyInfo: history,
	}
}
func (qpg *QuotaProjectGenerator) addHistory(path string, id int64) {
	qpg.mu.Lock()
	defer qpg.mu.Unlock()
	qpg.historyInfo[path] = id
}

func (qpg *QuotaProjectGenerator) removeHistory(path string) {
	qpg.mu.Lock()
	defer qpg.mu.Unlock()
	delete(qpg.historyInfo, path)
}
func (qpg *QuotaProjectGenerator) getNextProject(path string) (int64, string) {
	qpg.mu.Lock()
	defer qpg.mu.Unlock()

	if id, exists := qpg.historyInfo[getStandardPathString(path)]; exists == true {
		return id, fmt.Sprintf("%s%d", qpg.prifix, id)
	}
	qpg.nextid++
	return qpg.nextid, fmt.Sprintf("%s%d", qpg.prifix, qpg.nextid)
}

func (qpg *QuotaProjectGenerator) nextId() int64 {
	qpg.mu.Lock()
	defer qpg.mu.Unlock()
	return qpg.nextid
}

type NoneQuotaManager struct {
}

func (nqm *NoneQuotaManager) Init() error {
	return nil
}
func (nqm *NoneQuotaManager) GetQuotaDiskNum() int {
	return 0
}
func (nqm *NoneQuotaManager) GetQuotaDiskInfos() []DiskQuotaInfo {
	return []DiskQuotaInfo{}
}
func (nqm *NoneQuotaManager) IsPathXFSQuota(path string) (exists bool, info PathQuotaInfo) {
	return false, PathQuotaInfo{}
}
func (nqm *NoneQuotaManager) IsIdXFSQuota(id, subid string) (exists bool, info PathQuotaInfo) {
	return false, PathQuotaInfo{}
}
func (nqm *NoneQuotaManager) GetXFSQuotasById(id string) []PathQuotaInfo {
	return []PathQuotaInfo{}
}
func (nqm *NoneQuotaManager) AddPathQuota(ownerid, id, subid, addpath string, podid string, isUsedOld bool,
	sortQuota, hardQuota int64, activeFun ActiveFun) (ok bool, path string, err error) {
	return false, "", nil
}
func (nqm *NoneQuotaManager) ChangeQuota(id, subid string, newSortQuota, newHardQuota int64) (ok bool, err error) {
	return false, nil
}
func (nqm *NoneQuotaManager) DeletePathQuota(id, subid string) (ok bool, err error) {
	return false, nil
}
func (nqm *NoneQuotaManager) DeletePathQuotaByPath(path string) (ok bool, err error) {
	return false, nil
}
func (nqm *NoneQuotaManager) CleanOrphanQuotaPath(activePaths map[string]bool, activeFun ActiveFun) error {
	return nil
}
func (nqm *NoneQuotaManager) GetQuotaStatus() QuotaStatus {
	return QuotaStatus{
		DiskStatus: make([]DiskQuotaStatus, 0),
	}
}
func (nqm *NoneQuotaManager) StartQuotaStatusSync() bool {
	return true
}

func (nqm *NoneQuotaManager) StopQuotaStatusSync() bool {
	return true
}

func (nqm *NoneQuotaManager) IsQuotaDiskDisabled(diskpath string) bool {
	return false
}
func (nqm *NoneQuotaManager) SetQuotaDiskDisabled(diskpath string, disabled bool) error {
	return nil
}
func (nqm *NoneQuotaManager) IsQuotaPathExistAndUsed(quotaPath string, activeFun ActiveFun) (exist, used bool, err error) {
	return false, false, nil
}

type DiskQuotaManager interface {
	Init() error
	GetQuotaDiskNum() int
	GetQuotaDiskInfos() []DiskQuotaInfo
	IsPathXFSQuota(path string) (exists bool, info PathQuotaInfo)
	IsIdXFSQuota(id, subid string) (exists bool, info PathQuotaInfo)
	GetXFSQuotasById(id string) []PathQuotaInfo
	IsQuotaPathExistAndUsed(quotaPath string, activeFun ActiveFun) (exist, used bool, err error)
	AddPathQuota(ownerid, id, subid, addpath string, podid string, isUsedOld bool, sortQuota, hardQuota int64, activeFun ActiveFun) (ok bool, path string, err error)
	ChangeQuota(id, subid string, newSortQuota, newHardQuota int64) (ok bool, err error)
	DeletePathQuota(id, subid string) (ok bool, err error)
	DeletePathQuotaByPath(path string) (ok bool, err error)
	CleanOrphanQuotaPath(activePaths map[string]bool, activeFun ActiveFun) error

	GetQuotaStatus() QuotaStatus
	StartQuotaStatusSync() bool
	StopQuotaStatusSync() bool
	IsQuotaDiskDisabled(diskpath string) bool
	SetQuotaDiskDisabled(diskpath string, disabled bool) error
}

type PathQuotaInfo struct {
	Path        string
	PodId       string
	Id          string
	SubId       string
	KeepId      string
	OwnerId     string
	ProjectId   int64
	ProjectName string
	UsedSize    int64
	SoftQuota   int64
	HardQuota   int64
}

func (pqi *PathQuotaInfo) GetPath() string {
	if pqi.KeepId != "" {
		return path.Clean(pqi.Path) + "/" + xfsKeepForOnePodInnerDir
	} else {
		return path.Clean(pqi.Path) + "/" + xfsKeepForOnePodInnerDir
	}
}
func (pqi *PathQuotaInfo) GetDirName() string {
	if pqi.KeepId != "" {
		return fmt.Sprintf("%s_%s_%s", xfsQuotaDirPrifix, pqi.Id, pqi.KeepId)
	} else if pqi.SubId != "" {
		return fmt.Sprintf("%s_%s_%s", xfsQuotaDirPrifix, pqi.Id, pqi.SubId)
	} else {
		return fmt.Sprintf("%s_%s", xfsQuotaDirPrifix, pqi.Id)
	}
}
func getPVKeepForOnePodPrefix(id string) string {
	return xfsQuotaDirPrifix + "_" + id + "_" + xfsKeepForOnePodFlag + "-"
}
func getKeepIdFromKeepForOnePodDirName(basename string) string {
	strs := strings.Split(basename, "_")
	if len(strs) != 3 || strs[0] != xfsQuotaDirPrifix || strings.HasPrefix(strs[2], xfsKeepForOnePodFlag) == false {
		return ""
	} else {
		return strs[2]
	}
}
func createKeepIdForKeepForOnePod(ownerid string) string {
	return xfsKeepForOnePodFlag + "-" + ownerid
}

func getOwinderIdFilePath(dir string) string {
	dir = path.Clean(dir)
	name := path.Base(dir)
	dir = path.Dir(dir)
	return path.Join(dir, xfsOwnerIDSaveDirName, name)
}

func setOwnerId(dir, ownerId string, writeFun func(path, data string) error) error {
	fp := getOwinderIdFilePath(dir)
	return writeFun(fp, ownerId)
}

func getOwnerId(dir string, readFun func(path string) string) string {
	var ret string
	ret = readFun(getOwinderIdFilePath(dir))
	if ret != "" {
		return ret
	}
	// old version the ownerid file is save as (path + "/" + xfsKeepForOnePodSubIdFile)
	ret = readFun(path.Join(dir, xfsKeepForOnePodSubIdFile))
	return ret
}
func cleanOwnerIds(baseDir string, activeDirNames map[string]struct{}, activeFun ActiveFun, readFun func(path string) string) error {
	oweridDir := path.Join(baseDir, xfsOwnerIDSaveDirName)
	entries, err := ioutil.ReadDir(oweridDir)
	if err != nil {
		return fmt.Errorf("ReadDir %s err:%v", oweridDir, err)
	}
	errstrings := make([]string, 0, len(entries))
	for _, entry := range entries {
		if _, exist := activeDirNames[entry.Name()]; exist == false {
			filePath := path.Join(oweridDir, entry.Name())
			if activeFun(readFun(filePath), "") == false {
				if err := os.Remove(filePath); err != nil {
					errstrings = append(errstrings, fmt.Sprintf("remove ownerid file %s err:%v", filePath, err))
				}
			}
		}
	}
	if len(errstrings) == 0 {
		return nil
	} else {
		return fmt.Errorf("%s", strings.Join(errstrings, ","))
	}
}

func getInfoFromDirName(path string, readFun func(path string) string) (id, subid, keepid string, err error) {
	name := getFileNameFromDir(path)
	strs := strings.Split(name, "_")
	if (len(strs) != 2 && len(strs) != 3) || strs[0] != xfsQuotaDirPrifix {
		return "", "", "", fmt.Errorf("[%s] is not valid k8s xfs quota dir", name)
	}
	if len(strs) == 2 {
		return strs[1], "", "", nil
	} else if len(strs) == 3 && strings.HasPrefix(strs[2], xfsKeepForOnePodFlag) {
		return strs[1], getOwnerId(path, readFun), strs[2], nil
	}
	return strs[1], strs[2], "", nil
}

type DiskQuotaInfo struct {
	Capacity       int64
	UsedSize       int64
	AvailSize      int64
	SaveSize       int64
	QuotaedSize    int64
	MountPath      string
	Disabled       bool
	PathQuotaInfos map[string][]PathQuotaInfo
}

type XFSQuotaManager struct {
	runner                utilequota.Interface
	disks                 []DiskQuotaInfo
	projectGenerator      *QuotaProjectGenerator
	checkPath             string
	mu                    sync.Mutex
	xfsProjectInfoList    utilequota.XFSProjectInfoList
	xfsProjIdInfoList     utilequota.XFSProjIdInfoList
	xfsHistoryProjectList map[string]int64
	lastSaveHistoryInfo   string
	quotaMountPaths       []string // all the node mounted xfs quota path
	podKeepIdMap          map[string]string
	quotaStatus           QuotaStatus
	quotaStatusSync       chan struct{}
	quotaProjectChanged   bool
}

type QuotaDiskSort struct {
	paths          []string
	disks          []DiskQuotaInfo
	diskOwnerIdSet map[string]sets.String
	diskCanUseSize map[string]int64
	ownerid        string
	id             string
	subid          string
}

func newQuotaDiskSort(paths []string, disks []DiskQuotaInfo, ownerid, id, subid string) *QuotaDiskSort {
	ret := &QuotaDiskSort{
		paths:          paths,
		disks:          disks,
		ownerid:        ownerid,
		id:             id,
		subid:          subid,
		diskOwnerIdSet: make(map[string]sets.String),
		diskCanUseSize: make(map[string]int64),
	}
	for _, disk := range disks {
		set := sets.NewString()
		for _, infos := range disk.PathQuotaInfos {
			if len(infos) > 0 {
				set.Insert(infos[0].OwnerId) // the ownerid in infos are the same
				set.Insert(infos[0].Id)
			}
			for _, info := range infos {
				if info.SubId != "" {
					set.Insert(info.SubId)
				}
			}
		}
		ret.diskOwnerIdSet[disk.MountPath] = set
		ret.diskCanUseSize[disk.MountPath] = disk.Capacity - disk.SaveSize - disk.QuotaedSize

	}
	return ret
}
func (qds *QuotaDiskSort) getConflictLevel(path string) int {
	if qds.diskOwnerIdSet[path].Has(qds.subid) == true {
		return 3
	} else if qds.diskOwnerIdSet[path].Has(qds.id) || qds.diskOwnerIdSet[path].Has(qds.ownerid) {
		return 2
	}
	return 0
}
func (qds *QuotaDiskSort) Len() int { return len(qds.paths) }
func (qds *QuotaDiskSort) Less(i, j int) bool {
	leveli := qds.getConflictLevel(qds.paths[i])
	levelj := qds.getConflictLevel(qds.paths[j])
	if leveli != levelj {
		return leveli > levelj
	}
	return qds.diskCanUseSize[qds.paths[i]] < qds.diskCanUseSize[qds.paths[j]]
}
func (qds *QuotaDiskSort) Swap(i, j int) { qds.paths[i], qds.paths[j] = qds.paths[j], qds.paths[i] }

func NewXFSQuotaManager(checkPath string, exec utilexec.Interface) DiskQuotaManager {
	ret := &XFSQuotaManager{
		disks:            make([]DiskQuotaInfo, 0),
		projectGenerator: nil,
		checkPath:        checkPath,
		runner:           utilequota.New(exec),
		quotaMountPaths:  make([]string, 0),
		podKeepIdMap:     make(map[string]string),
		quotaStatus: QuotaStatus{
			DiskStatus: make([]DiskQuotaStatus, 0),
		},
		quotaStatusSync: make(chan struct{}),
	}
	if err := ret.Init(); err != nil {
		glog.Errorf("NewXFSQuotaManager init error :%v", err)
		return &NoneQuotaManager{}
	} else {
		return ret
	}
}

func (xfsqm *XFSQuotaManager) IsQuotaDiskDisabled(diskpath string) bool {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	return xfsqm.isQuotaDiskDisabled(diskpath, true)
}
func (xfsqm *XFSQuotaManager) isQuotaDiskDisabled(diskpath string, check bool) bool {
	diskpath = getStandardPathString(diskpath)
	if check {
		var find bool
		for _, disk := range xfsqm.disks {
			if getStandardPathString(disk.MountPath) == diskpath {
				find = true
				break
			}
		}
		if find == false { // not find disk is disabled
			return true
		}
	}
	disable := xfsqm.runner.ReadFile(path.Join(diskpath, xfsDiskDisablefilename))
	if disable == "yes" {
		return true
	}
	return false
}
func (xfsqm *XFSQuotaManager) SetQuotaDiskDisabled(diskpath string, disabled bool) error {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	return xfsqm.setQuotaDiskDisabled(diskpath, disabled)
}
func (xfsqm *XFSQuotaManager) setQuotaDiskDisabled(diskpath string, disabled bool) error {
	diskpath = getStandardPathString(diskpath)
	var find bool
	var index int
	for i, disk := range xfsqm.disks {
		if getStandardPathString(disk.MountPath) == diskpath {
			find = true
			index = i
			break
		}
	}
	if find == false {
		return fmt.Errorf("disk %s is not quota disk", diskpath)
	}
	xfsqm.disks[index].Disabled = disabled
	var disablestr string = "no"
	if disabled == true {
		disablestr = "yes"
	}
	return xfsqm.runner.WriteFile(path.Join(diskpath, xfsDiskDisablefilename), disablestr)
}

func (xfsqm *XFSQuotaManager) Init() error {
	ok, err := checkSystemSupportXFSQuota()
	if ok != true || err != nil {
		return fmt.Errorf("Init checkSystemSupportXFSQuota error=%v", err)
	}
	if ok, err = xfsqm.runner.CurSystemSupport(); ok != true || err != nil {
		return fmt.Errorf("xfs_quota not support %v", err)
	}
	if ok = xfsqm.runner.IsHostQuotaPathExist(xfsqm.checkPath); ok == false {
		return fmt.Errorf("xfs_quota mount path %s is not exist", xfsqm.checkPath)
	}
	if err = xfsqm.initQuotaXFSDisk(); err != nil {
		return fmt.Errorf("initQuotaXFSDisk error =%v", err)
	}
	glog.Infof("XFSQuotaManager init quota disk:%v", xfsqm.disks)
	return nil
}

func (xfsqm *XFSQuotaManager) refreshProjectInfo() (int64, error) {
	var maxId int64 = -1
	var csiStartId int64 = 10000
	projectInfo := make(map[int64]bool)
	var projects utilequota.XFSProjectInfoList
	var projid utilequota.XFSProjIdInfoList
	if projectinfo, err := xfsqm.runner.QuotaProjects(); err != nil {
		return maxId, fmt.Errorf("refreshProjectInfo get QuotaProjects error:%v", err)
	} else {
		xfsProjectInfoList := utilequota.DecodeProjectInfo(projectinfo)
		list := make(utilequota.XFSProjectInfoList, 0, len(xfsProjectInfoList))
		for _, info := range xfsProjectInfoList {
			if int64(info.ProjectId) < csiStartId && int64(info.ProjectId) > maxId {
				maxId = int64(info.ProjectId)
			}
			if xfsqm.runner.IsHostQuotaPathExist(string(info.Path)) { // check if the quota path is exist
				projectInfo[int64(info.ProjectId)] = true
				list = append(list, info)
			}
		}
		projects = list
	}

	if projectidinfo, err := xfsqm.runner.QuotaProjid(); err != nil {
		return maxId, fmt.Errorf("refreshProjectInfo get QuotaProjid error:%v", err)
	} else {
		xfsProjIdInfoList := utilequota.DecodeProjIdInfo(projectidinfo)
		list := make(utilequota.XFSProjIdInfoList, 0, len(xfsProjIdInfoList))
		for _, info := range xfsProjIdInfoList {
			if int64(info.ProjectId) < csiStartId && int64(info.ProjectId) > maxId {
				maxId = int64(info.ProjectId)
			}
			if _, exists := projectInfo[int64(info.ProjectId)]; exists {
				list = append(list, info)
			}
		}
		projid = list
	}

	// save the project info
	if err := xfsqm.runner.SaveQuotaProjectsAndId(projects.ToString(), projid.ToString()); err != nil {
		return maxId, fmt.Errorf("refreshProjectInfo SaveQuotaProjectsAndId error %v", err)
	}
	xfsqm.xfsProjectInfoList = projects
	xfsqm.xfsProjIdInfoList = projid
	return maxId, nil
}
func getProjectQuotaPaths(stateInfos []utilequota.XFSStateInfo) []string {
	ret := make([]string, 0, len(stateInfos))
	for _, state := range stateInfos {
		if state.ProjectQuotaAccountingOn == true && state.ProjectQuotaEnforcementOn == true {
			ret = append(ret, string(state.MountPath))
		}
	}
	return ret
}
func updateDiskInfo(disk *DiskQuotaInfo) {
	if disk != nil {
		var quotaSum int64
		for _, infos := range disk.PathQuotaInfos {
			for _, info := range infos {
				quotaSum += info.HardQuota
			}
		}
		disk.QuotaedSize = quotaSum
		disk.AvailSize = disk.Capacity - disk.SaveSize - disk.QuotaedSize
	}
}
func (xfsqm *XFSQuotaManager) initQuotaXFSDisk() error {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	curMaxId, _ := xfsqm.refreshProjectInfo()
	historyId, _ := xfsqm.loadHistoryInfo()

	if stateinfo, err := xfsqm.runner.QuotaState(); err != nil {
		return fmt.Errorf("initQuotaXFSDisk get state info error %v", err)
	} else {
		states := utilequota.DecodeStateInfo(stateinfo)
		xfsqm.quotaMountPaths = getProjectQuotaPaths(states)
		xfsqm.disks = make([]DiskQuotaInfo, 0, len(states))
		if usedInfo, _ := xfsqm.runner.QuotaUsedInfo(xfsqm.quotaMountPaths); /*err == nil*/ true /*we skip the error which make no difference*/ {
			usedState := utilequota.DecodeUsedInfo(usedInfo)
			diskPathAdded := make(map[string]bool)
			for _, state := range states {
				if state.ProjectQuotaAccountingOn == true && state.ProjectQuotaEnforcementOn == true &&
					strings.HasPrefix(getStandardPathString(string(state.MountPath)), getStandardPathString(xfsqm.checkPath)) {
					var quotaedSize int64 = 0
					var disk *DiskQuotaInfo = nil
					for _, us := range usedState {
						if getStandardPathString(string(us.Device)) == getStandardPathString(string(state.Device)) {
							if getStandardPathString(string(us.MountPath)) == getStandardPathString(string(state.MountPath)) {
								disk = &DiskQuotaInfo{
									Capacity:       us.Size * 1024,
									UsedSize:       us.Used * 1024,
									AvailSize:      (us.Size - us.Used) * 1024,
									SaveSize:       xfsDefaultSaveSize,
									QuotaedSize:    0,
									Disabled:       xfsqm.isQuotaDiskDisabled(string(state.MountPath), false),
									MountPath:      getStandardPathString(string(state.MountPath)),
									PathQuotaInfos: make(map[string][]PathQuotaInfo),
								}
							} else {
								quotaedSize += us.Size * 1024
							}
						}
					}
					if disk != nil {
						disk.QuotaedSize = quotaedSize
						if _, exist := diskPathAdded[disk.MountPath]; exist == false {
							diskPathAdded[string(disk.MountPath)] = true
							xfsqm.disks = append(xfsqm.disks, *disk)
						}
					}
				}
			}
		}

		if len(xfsqm.disks) == 0 {
			return fmt.Errorf("initQuotaXFSDisk no xfs disk project quota is on")
		}
	}
	var maxProjectId int64 = 0
	projectInfo := make(map[int64]bool)
	if projectinfo, err := xfsqm.runner.QuotaProjects(); err != nil {
		xfsqm.xfsProjectInfoList = make(utilequota.XFSProjectInfoList, 0)
	} else {
		xfsProjectInfoList := utilequota.DecodeProjectInfo(projectinfo)
		list := make(utilequota.XFSProjectInfoList, 0, len(xfsProjectInfoList))
		addedProject := make(map[int64]struct{})
		for _, info := range xfsProjectInfoList {
			if int64(info.ProjectId) > maxProjectId {
				maxProjectId = int64(info.ProjectId)
			}
			if xfsqm.runner.IsHostQuotaPathExist(string(info.Path)) { // check if the quota path is exist
				if _, added := addedProject[int64(info.ProjectId)]; added == false {
					addedProject[int64(info.ProjectId)] = struct{}{}
					list = append(list, info)
					projectInfo[int64(info.ProjectId)] = true
				}
			}
		}
		xfsqm.xfsProjectInfoList = list
	}

	if projectidinfo, err := xfsqm.runner.QuotaProjid(); err != nil {
		xfsqm.xfsProjIdInfoList = make(utilequota.XFSProjIdInfoList, 0)
	} else {
		xfsProjIdInfoList := utilequota.DecodeProjIdInfo(projectidinfo)
		list := make(utilequota.XFSProjIdInfoList, 0, len(xfsProjIdInfoList))
		addedProject := make(map[int64]struct{})
		for _, info := range xfsProjIdInfoList {
			if int64(info.ProjectId) > maxProjectId {
				maxProjectId = int64(info.ProjectId)
			}
			if _, exists := projectInfo[int64(info.ProjectId)]; exists {
				if _, added := addedProject[int64(info.ProjectId)]; added == false {
					addedProject[int64(info.ProjectId)] = struct{}{}
					list = append(list, info)
				}
			}
		}
		xfsqm.xfsProjIdInfoList = list
	}
	// save the project info
	if err := xfsqm.runner.SaveQuotaProjectsAndId(xfsqm.xfsProjectInfoList.ToString(), xfsqm.xfsProjIdInfoList.ToString()); err != nil {
		return fmt.Errorf("SaveQuotaProjectsAndId error %v", err)
	}
	if maxProjectId < xfsQuotaProjIdStart {
		maxProjectId = xfsQuotaProjIdStart
	}
	if maxProjectId < curMaxId {
		maxProjectId = curMaxId
	}
	if maxProjectId < historyId {
		maxProjectId = historyId
	}
	xfsqm.projectGenerator = newQuotaProjectGenerator(maxProjectId, xfsQuotaProjectPrifix, xfsqm.xfsHistoryProjectList)

	if reportInfo, err := xfsqm.runner.QuotaReport(xfsqm.quotaMountPaths); err != nil {
		return fmt.Errorf("initQuotaXFSDisk get QuotaReport info error %v", err)
	} else {
		report := utilequota.DecodeReportInfo(reportInfo)

		getProjectNameById := func(id utilequota.XFSProjectID) string {
			for _, pidInfo := range xfsqm.xfsProjIdInfoList {
				if pidInfo.ProjectId == id {
					return string(pidInfo.ProjectName)
				}
			}
			return ""
		}
		for _, pro := range xfsqm.xfsProjectInfoList {
			projname := getProjectNameById(pro.ProjectId)
			id, subid, keepId, err := getInfoFromDirName(string(pro.Path), xfsqm.runner.ReadFile)
			if err == nil {
				if value, find := report[utilequota.XFSProjectName(projname)]; find {
					pathInfo := PathQuotaInfo{
						Path:        string(pro.Path),
						Id:          id,
						SubId:       subid,
						KeepId:      keepId,
						OwnerId:     "",
						ProjectId:   int64(pro.ProjectId),
						ProjectName: projname,
						UsedSize:    int64(value.Used) * 1024, // report unit is 1KB
						SoftQuota:   int64(value.Soft) * 1024, // report unit is 1KB
						HardQuota:   int64(value.Hard) * 1024, // report unit is 1KB
					}
					for i, _ := range xfsqm.disks {
						if strings.HasPrefix(string(pro.Path), xfsqm.disks[i].MountPath) {
							xfsqm.disks[i].PathQuotaInfos[id] = append(xfsqm.disks[i].PathQuotaInfos[id], pathInfo)
							break
						}
					}
				}
			}
		}
	}
	return nil
}
func (xfsqm *XFSQuotaManager) GetQuotaDiskNum() int {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()

	return len(xfsqm.disks)
}
func (xfsqm *XFSQuotaManager) GetQuotaDiskInfos() []DiskQuotaInfo {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	return xfsqm.disks

	ret := make([]DiskQuotaInfo, 0, len(xfsqm.disks))

	deepCopy(ret, xfsqm.disks)

	return ret
}
func (xfsqm *XFSQuotaManager) IsPathXFSQuota(path string) (bool, PathQuotaInfo) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()

	return xfsqm.isPathXFSQuota(path)
}
func (xfsqm *XFSQuotaManager) isPathXFSQuota(path string) (bool, PathQuotaInfo) {
	path = getStandardPathString(path)

	for _, disk := range xfsqm.disks {
		for _, infos := range disk.PathQuotaInfos {
			for _, info := range infos {
				if path == getStandardPathString(info.Path) {
					return true, info
				}
			}
		}
	}
	return false, PathQuotaInfo{}
}

// return all the projquotas which have the same id
func (xfsqm *XFSQuotaManager) GetXFSQuotasById(id string) []PathQuotaInfo {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	ret := make([]PathQuotaInfo, 0)
	for _, disk := range xfsqm.disks {
		if infos, ok := disk.PathQuotaInfos[id]; ok {
			ret = append(ret, infos...)
		}
	}
	return ret
}

func (xfsqm *XFSQuotaManager) IsIdXFSQuota(id, subid string) (bool, PathQuotaInfo) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	return xfsqm.isIdXFSQuota(id, subid)
}
func (xfsqm *XFSQuotaManager) isIdXFSQuota(id, subid string) (bool, PathQuotaInfo) {
	for _, disk := range xfsqm.disks {
		if infos, ok := disk.PathQuotaInfos[id]; ok {
			for _, info := range infos {
				if info.SubId == subid {
					return true, info
				}
			}
		}
	}
	return false, PathQuotaInfo{}
}
func (xfsqm *XFSQuotaManager) updateQuotaInfo(id, subid string, updateInfo PathQuotaInfo) bool {
	xfsqm.addHistoryInfo(updateInfo.Path, updateInfo.ProjectId)
	for diskindex, disk := range xfsqm.disks {
		if infos, ok := disk.PathQuotaInfos[id]; ok {
			for i, info := range infos {
				if info.SubId == subid {
					xfsqm.disks[diskindex].QuotaedSize = xfsqm.disks[diskindex].QuotaedSize - info.HardQuota + updateInfo.HardQuota
					xfsqm.disks[diskindex].PathQuotaInfos[id][i] = updateInfo
					return true
				}
			}
			return false
		}
	}
	return false
}

func (xfsqm *XFSQuotaManager) updateQuotaPathSubId(path, subid string) {
	path = getStandardPathString(path)

	for diskindex, disk := range xfsqm.disks {
		for id, infos := range disk.PathQuotaInfos {
			for i, info := range infos {
				if path == getStandardPathString(info.Path) {
					xfsqm.disks[diskindex].PathQuotaInfos[id][i].SubId = subid
					return
				}
			}
		}
	}
}

func (xfsqm *XFSQuotaManager) ChangeQuota(id, subid string, newSortQuota, newHardQuota int64) (ok bool, err error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	ok, err = xfsqm.changeQuota(id, subid, newSortQuota, newHardQuota)
	return
}
func (xfsqm *XFSQuotaManager) changeQuota(id, subid string, newSortQuota, newHardQuota int64) (ok bool, err error) {
	defer xfsqm.quotaChanged()
	ok, info := xfsqm.isIdXFSQuota(id, subid)
	if ok {
		if info.HardQuota > newHardQuota { // smaller quota
			_, cursize := GetDirFileSize(info.Path)
			if cursize > newHardQuota {
				return false, fmt.Errorf("current used %d cannot quota %d", cursize, newHardQuota)
			}
			if _, err, s, h := xfsqm.runner.LimitQuotaProject(info.ProjectName, newSortQuota, newHardQuota); err != nil {
				return false, fmt.Errorf("cannot quota %s:%s to %d, %v", id, info.Path, newHardQuota, err)
			} else {
				info.SoftQuota = s
				info.HardQuota = h
				xfsqm.updateQuotaInfo(id, subid, info)
			}
		} else { // bigger quota always do it
			if _, err, s, h := xfsqm.runner.LimitQuotaProject(info.ProjectName, newSortQuota, newHardQuota); err != nil {
				return false, fmt.Errorf("cannot quota %s:%s to %d, %v", id, info.Path, newHardQuota, err)
			} else {
				info.SoftQuota = s
				info.HardQuota = h
				xfsqm.updateQuotaInfo(id, subid, info)
			}
		}
		return true, nil
	}
	return false, fmt.Errorf("ChangeQuota id:%s, subid=%s is not exists", id, subid)
}
func (xfsqm *XFSQuotaManager) getPathsInQuotaDisk() []string {
	allExistPaths := make([]string, 0, 100)
	for _, disk := range xfsqm.disks {
		allExistPaths = append(allExistPaths, xfsqm.runner.GetSubDirs(disk.MountPath)...)
	}
	return allExistPaths
}

func (xfsqm *XFSQuotaManager) getQuotaPath(defaultDiskPath, dirname string) string {
	for _, disk := range xfsqm.disks {
		if xfsqm.runner.IsHostQuotaPathExist(getStandardPathString(disk.MountPath) + dirname) {
			return getStandardPathString(disk.MountPath) + dirname + "/"
		}
	}
	return getStandardPathString(defaultDiskPath) + dirname + "/"
}
func (xfsqm *XFSQuotaManager) isKeepIdUsedByOthers(keepId, subid string, activeFun ActiveFun) bool {
	for _, disk := range xfsqm.disks {
		for _, infos := range disk.PathQuotaInfos {
			for _, info := range infos {
				if strings.HasSuffix(path.Base(info.Path), keepId) {
					infoSubId := getOwnerId(info.Path, xfsqm.runner.ReadFile) //xfsqm.runner.ReadFile(info.Path + "/" + xfsKeepForOnePodSubIdFile) //getSubIdFromDir(info.Path)
					if infoSubId != subid && activeFun(infoSubId, "") {
						return true
					}
				}
			}
		}
	}
	return false
}
func (xfsqm *XFSQuotaManager) getKeepId(oid, id string, activeFun ActiveFun) (keepid, hostpath string) {
	// if the id has created a subid before just return
	existid, exist := xfsqm.podKeepIdMap[oid]
	if exist == true && existid != "" {
		return existid, ""
	}
	// check if has one dir which is created by the id and not using now
	existedPaths := xfsqm.getPathsInQuotaDisk()
	findPrefix := getPVKeepForOnePodPrefix(id)
	for _, dir := range existedPaths {
		basename := path.Base(dir)
		if strings.HasPrefix(basename, findPrefix) {
			keepid := getKeepIdFromKeepForOnePodDirName(basename)
			// if we have PV1_keepid1 PV2_keepid1 and PV1_keepid1 is used then
			// PV2_keepid1 can not be used
			if keepid != "" && xfsqm.isKeepIdUsedByOthers(keepid, oid, activeFun) == false {
				xfsqm.podKeepIdMap[oid] = keepid
				return keepid, dir
			}
		}
	}
	// to this step we should create a new subid
	newkeepid := createKeepIdForKeepForOnePod(oid)
	xfsqm.podKeepIdMap[oid] = newkeepid
	return newkeepid, ""
}

// id+subid is key for quota path
// make sure the same owner id will be assigned to different disk
func (xfsqm *XFSQuotaManager) AddPathQuota(ownerid, id, subid, addpath string, podid string, isUsedOld bool, softQuota, hardQuota int64, activeFun ActiveFun) (ok bool, mountpath string, err error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	defer xfsqm.quotaChanged()
	/*
	 isUsedOld == true means the pv annotation
	 io.enndata.user/alpha-pvhostpathmountpolicy:keep
	 io.enndata.kubelet/alpha-pvhostpathquotaforonepod:true
	*/
	xfsqm.refreshProjectInfo()
	var keepId, hostpath string
	if isUsedOld == true {
		keepId, hostpath = xfsqm.getKeepId(subid, id, activeFun)
	}

	canUseDiskPath := make([]string, 0, len(xfsqm.disks))
	for _, disk := range xfsqm.disks {
		if pqis, exists := disk.PathQuotaInfos[id]; exists == true {
			for _, pqi := range pqis {
				if pqi.SubId == subid { // the id + subid is used which is not allow be added repeatedly
					return true, pqi.GetPath(), fmt.Errorf("id:%s subid:%s is mounted", id, subid)
				}
			}
		}
		// if addpath is not empty means the disk mount path must equal to it
		if addpath != "" && getStandardPathString(disk.MountPath) != getStandardPathString(addpath) {
			continue
		}
		if xfsqm.isQuotaDiskDisabled(disk.MountPath, false) == true { // disk is disabled
			continue
		}
		// the disk available space greater than the request
		if disk.Capacity >= disk.SaveSize+disk.QuotaedSize+hardQuota {
			canUseDiskPath = append(canUseDiskPath, disk.MountPath)
		}
	}
	pathQuotaInfo := PathQuotaInfo{
		Id:        id,
		PodId:     podid,
		OwnerId:   ownerid,
		SubId:     subid,
		KeepId:    keepId,
		UsedSize:  0,
		SoftQuota: softQuota,
		HardQuota: hardQuota,
	}
	recommendPath := xfsqm.getQuotaPath("", pathQuotaInfo.GetDirName())
	var useDiskPath string
	if hostpath != "" && xfsqm.runner.IsHostQuotaPathExist(hostpath) == true {
		// hostpath is used by no pod and occupied the disk quota, so it should not calculate the quota
		useDiskPath = getStandardPathString(path.Dir(hostpath))
	} else if recommendPath != "" && xfsqm.runner.IsHostQuotaPathExist(recommendPath) == true {
		useDiskPath = getStandardPathString(path.Dir(recommendPath))
	} else {
		if len(canUseDiskPath) == 0 {
			return false, "", fmt.Errorf("has no disk can match the hardquota %d", hardQuota)
		}
		if len(canUseDiskPath) == 1 {
			useDiskPath = canUseDiskPath[0]
		} else {
			// make sure the same ownerid , id and subid quota path assigned to different disk as far as possible
			s := newQuotaDiskSort(canUseDiskPath, xfsqm.disks, ownerid, id, subid)
			sort.Sort(sort.Reverse(s))
			useDiskPath = s.paths[0]
		}
	}
	diskPath := getStandardPathString(useDiskPath)

	pathQuotaInfo.Path = xfsqm.getQuotaPath(diskPath, pathQuotaInfo.GetDirName())
	pathQuotaInfo.ProjectId, pathQuotaInfo.ProjectName = xfsqm.projectGenerator.getNextProject(pathQuotaInfo.Path)
	if xfsqm.runner.IsHostQuotaPathExist(pathQuotaInfo.Path) == false {
		if _, err := xfsqm.runner.MkQuotaDir(pathQuotaInfo.Path, xfsKeepForOnePodInnerDir); err != nil {
			return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathQuota MkQuotaDir %s error %v", pathQuotaInfo.Path, err)
		}
	} else if exist, _ := xfsqm.isPathXFSQuota(pathQuotaInfo.Path); exist {
		xfsqm.updateQuotaPathSubId(pathQuotaInfo.Path, subid) // this path maybe used by other deleted pod, we should change the subid before changeQuota

		if isUsedOld == true {
			seterr := setOwnerId(pathQuotaInfo.Path, subid, xfsqm.runner.WriteFile) //xfsqm.runner.WriteFile(pathQuotaInfo.Path+"/"+xfsKeepForOnePodSubIdFile, subid)
			//seterr := setSubIdToDir(pathQuotaInfo.Path, subid)
			if seterr != nil {
				return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathQuota setSubIdToDir %s error %v", pathQuotaInfo.Path, seterr)
			}
		}
		ok, err := xfsqm.changeQuota(id, subid, softQuota, hardQuota)
		if ok == false || err != nil {
			return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathQuota changeQuota %s error %v", pathQuotaInfo.Path, err)
		}
		return true, pathQuotaInfo.GetPath(), nil
	}
	if isUsedOld == true {
		seterr := setOwnerId(pathQuotaInfo.Path, subid, xfsqm.runner.WriteFile) //xfsqm.runner.WriteFile(pathQuotaInfo.Path+"/"+xfsKeepForOnePodSubIdFile, subid)
		//seterr := setSubIdToDir(pathQuotaInfo.Path, subid)
		if seterr != nil {
			return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathQuota setSubIdToDir %s error %v", pathQuotaInfo.Path, seterr)
		}
	}

	xfsqm.xfsProjectInfoList.Add(utilequota.XFSProjectID(pathQuotaInfo.ProjectId), utilequota.XFSProjectPath(pathQuotaInfo.Path))
	xfsqm.xfsProjIdInfoList.Add(utilequota.XFSProjectID(pathQuotaInfo.ProjectId), utilequota.XFSProjectName(pathQuotaInfo.ProjectName))
	// add a xfs prjquota
	// echo "1:/xfs/disk2/k8squota_9c76a178-0858-11e7-9d21-1866da26c25d_9f8083b3-0858-11e7-9d21-1866da26c25d" >> /etc/projects
	// echo "k8spro1:1" >> /etc/projid
	// xfs_quota -x -c "project -s k8spro1"
	// xfs_quota -x -c "limit -p bsoft=50m bhard=100m k8spro1"

	// step1: add projectid , projectname and projectpath to /etc/projects and /etc/projid
	if err := xfsqm.runner.SaveQuotaProjectsAndId(xfsqm.xfsProjectInfoList.ToString(), xfsqm.xfsProjIdInfoList.ToString()); err != nil {
		xfsqm.xfsProjectInfoList.DeleteById(utilequota.XFSProjectID(pathQuotaInfo.ProjectId))
		xfsqm.xfsProjIdInfoList.DeleteById(utilequota.XFSProjectID(pathQuotaInfo.ProjectId))
		return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathQuota SaveQuotaProjectsAndId error %v", err)
	}
	// step2: enable the project by projectname
	if buf, err := xfsqm.runner.SetupQuotaProject(pathQuotaInfo.ProjectName, []string{diskPath}); err != nil {
		xfsqm.xfsProjectInfoList.DeleteById(utilequota.XFSProjectID(pathQuotaInfo.ProjectId))
		xfsqm.xfsProjIdInfoList.DeleteById(utilequota.XFSProjectID(pathQuotaInfo.ProjectId))
		return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathQuota SetupQuotaProject %s error[%s] %v", pathQuotaInfo.ProjectName, string(buf), err)
	}
	// step3: limit the project quota by projectname
	if buf, err, sq, hq := xfsqm.runner.LimitQuotaProject(pathQuotaInfo.ProjectName, softQuota, hardQuota); false && err != nil {
		// we ignore the error here because if node has xfs disk not config prjquota will has error return but not affect the quota
		xfsqm.xfsProjectInfoList.DeleteById(utilequota.XFSProjectID(pathQuotaInfo.ProjectId))
		xfsqm.xfsProjIdInfoList.DeleteById(utilequota.XFSProjectID(pathQuotaInfo.ProjectId))
		return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathQuota LimitQuotaProject %s:%d,%d forpath %s error[%s] %v",
			pathQuotaInfo.ProjectName, softQuota, hardQuota, pathQuotaInfo.Path, string(buf), err)
	} else {
		pathQuotaInfo.SoftQuota = sq
		pathQuotaInfo.HardQuota = hq
		for i, _ := range xfsqm.disks {
			if strings.HasPrefix(pathQuotaInfo.Path, xfsqm.disks[i].MountPath) {
				// save the projquota
				xfsqm.disks[i].PathQuotaInfos[id] = append(xfsqm.disks[i].PathQuotaInfos[id], pathQuotaInfo)
				xfsqm.addHistoryInfo(pathQuotaInfo.Path, pathQuotaInfo.ProjectId)
				updateDiskInfo(&xfsqm.disks[i])
				return true, pathQuotaInfo.GetPath(), nil
			}
		}
		return false, pathQuotaInfo.GetPath(), fmt.Errorf("AddPathquota save info error:%v", err)
	}
}

func (xfsqm *XFSQuotaManager) IsQuotaPathExistAndUsed(quotaPath string, activeFun ActiveFun) (exist, used bool, err error) {
	if exist := xfsqm.runner.IsHostQuotaPathExist(quotaPath); exist == false {
		return false, false, nil
	}

	if strings.Contains(quotaPath, xfsKeepForOnePodFlag) == false {
		return true, false, fmt.Errorf("%s is not keeptrue quota path", quotaPath)
	}
	curOwnerId := getOwnerId(quotaPath, xfsqm.runner.ReadFile)
	if activeFun(curOwnerId, "") {
		return true, true, nil
	}
	return true, false, nil
}

func (xfsqm *XFSQuotaManager) loadHistoryInfo() (int64, error) {
	var nextid int64 = -1
	xfsqm.xfsHistoryProjectList = make(map[string]int64)
	if buf, err := xfsqm.runner.HistoryQuota(); err != nil {
		return nextid, fmt.Errorf("loadHistoryInfo err:%v", err)
	} else {
		list := utilequota.DecodeProjectInfo(buf)
		for _, item := range list {
			xfsqm.xfsHistoryProjectList[getStandardPathString(string(item.Path))] = int64(item.ProjectId)
		}
		nextid, _ = xfsqm.xfsHistoryProjectList[xfsHistroyNextIdName]
		return nextid, nil
	}
}
func (xfsqm *XFSQuotaManager) saveHistoryInfo() {
	curInfo := xfsqm.getHistoryInfoString()
	if xfsqm.lastSaveHistoryInfo != curInfo {
		if err := xfsqm.runner.SaveHistoryQuotaInfo(xfsqm.getHistoryInfoString()); err == nil {
			xfsqm.lastSaveHistoryInfo = curInfo
		}
	}
}
func (xfsqm *XFSQuotaManager) addHistoryInfo(path string, id int64) {
	//xfsqm.projectGenerator.addHistory(path, id)
	path = getStandardPathString(path)
	xfsqm.xfsHistoryProjectList[path] = id
	xfsqm.saveHistoryInfo()
}

func (xfsqm *XFSQuotaManager) removeHistoryInfo(path string) {
	//xfsqm.projectGenerator.removeHistory(path)
	path = getStandardPathString(path)
	delete(xfsqm.xfsHistoryProjectList, path)
	xfsqm.saveHistoryInfo()
}

func (xfsqm *XFSQuotaManager) getHistoryInfoString() string {
	ret := ""
	for path, id := range xfsqm.xfsHistoryProjectList {
		if path != xfsHistroyNextIdName {
			ret += fmt.Sprintf("%d:%s\n", id, path)
		}
	}
	ret += fmt.Sprintf("%d:%s\n", xfsqm.projectGenerator.nextId(), xfsHistroyNextIdName)
	return ret
}
func (xfsqm *XFSQuotaManager) DeletePathQuotaByPath(path string) (ok bool, err error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	defer xfsqm.quotaChanged()
	xfsqm.refreshProjectInfo()
	path = getStandardPathString(path)

	for indexdisk, disk := range xfsqm.disks {
		for _, infos := range disk.PathQuotaInfos {
			for i, info := range infos {
				if path == getStandardPathString(info.Path) {
					if len(infos) == 1 {
						delete(xfsqm.disks[indexdisk].PathQuotaInfos, info.Id)
					}
					xfsqm.disks[indexdisk].PathQuotaInfos[info.Id] = append(infos[0:i], infos[i+1:]...)
					xfsqm.xfsProjectInfoList.DeleteById(utilequota.XFSProjectID(info.ProjectId))
					xfsqm.xfsProjIdInfoList.DeleteById(utilequota.XFSProjectID(info.ProjectId))
					if err := xfsqm.runner.SaveQuotaProjectsAndId(xfsqm.xfsProjectInfoList.ToString(), xfsqm.xfsProjIdInfoList.ToString()); err != nil {
						return false, fmt.Errorf("DeletePathQuotaByPath SaveQuotaProjectsAndId error %v", err)
					}
					updateDiskInfo(&xfsqm.disks[indexdisk])
					return true, nil
				}
			}
		}
	}

	return false, fmt.Errorf("delete quota path %s is not exists", path)
}

func (xfsqm *XFSQuotaManager) DeletePathQuota(id, subid string) (ok bool, err error) {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	defer xfsqm.quotaChanged()
	xfsqm.refreshProjectInfo()
	for indexdisk, disk := range xfsqm.disks {
		if infos, ok := disk.PathQuotaInfos[id]; ok {
			for i, info := range infos {
				if info.SubId == subid { // find the projquota which will be deleted
					if len(infos) == 1 {
						delete(xfsqm.disks[indexdisk].PathQuotaInfos, id)
					}
					xfsqm.disks[indexdisk].PathQuotaInfos[id] = append(infos[0:i], infos[i+1:]...)
					xfsqm.xfsProjectInfoList.DeleteById(utilequota.XFSProjectID(info.ProjectId))
					xfsqm.xfsProjIdInfoList.DeleteById(utilequota.XFSProjectID(info.ProjectId))
					// delete the project info from the /etc/projects and /etc/projid
					if err := xfsqm.runner.SaveQuotaProjectsAndId(xfsqm.xfsProjectInfoList.ToString(), xfsqm.xfsProjIdInfoList.ToString()); err != nil {
						return false, fmt.Errorf("DeletePathQuota SaveQuotaProjectsAndId error %v", err)
					}
					updateDiskInfo(&xfsqm.disks[indexdisk])
					return true, nil
				}
			}
		}
	}

	return false, fmt.Errorf("delete quota id %s is not exists", id)
}
func isConfigFile(name string) bool {
	if name == xfsDiskDisablefilename || name == xfsOwnerIDSaveDirName {
		return true
	}
	return false
}

func isCSIQuotaPath(baseDir, quotaDirName string, readFun func(path string) string) bool {
	if strings.Contains(strings.ToLower(quotaDirName), "csi") == true ||
		readFun(path.Join(baseDir, quotaDirName, xfsCSIFlagFile)) != "" ||
		readFun(path.Join(baseDir, xfsCSISaveDirName, quotaDirName)) != "" {
		return true
	}
	return false
}

func (xfsqm *XFSQuotaManager) CleanOrphanQuotaPath(activePaths map[string]bool, activeFun ActiveFun) error {
	//glog.Infof("CleanOrphanQuotaPath activePaths=%v", activePaths)
	//shouldDeleteQuotaPath := make([]PathQuotaInfo, 0)
	for _, disk := range xfsqm.disks {
		entries, err := ioutil.ReadDir(disk.MountPath)
		if err != nil {
			continue
		}
		activeDirNames := make(map[string]struct{})
		for _, entry := range entries {
			if isConfigFile(entry.Name()) {
				continue
			}
			if isCSIQuotaPath(disk.MountPath, entry.Name(), xfsqm.runner.ReadFile) { // CSI quota path will clean by itself
				continue
			}
			path := getStandardPathString(disk.MountPath) + entry.Name() + "/"
			save := true
			if notRecycle, exists := activePaths[path]; exists == false {
				if ok, info := xfsqm.IsPathXFSQuota(path); ok == true {
					infoPodId := getOwnerId(info.Path, xfsqm.runner.ReadFile) //xfsqm.runner.ReadFile(info.Path + "/" + xfsKeepForOnePodSubIdFile) //getSubIdFromDir(info.Path)
					if infoPodId == "" {                                      // for none true
						_, infoPodId, _, _ = getInfoFromDirName(path, xfsqm.runner.ReadFile)
					}
					if activeFun(infoPodId, info.Id) == false { // the pod is not active delete the quotapath
						xfsqm.DeletePathQuotaByPath(path)
						glog.Infof("CleanOrphanQuotaPath DeletePathQuotaByPath %s,pod %s is not active", path, infoPodId)
					} else {
						glog.Infof("CleanOrphanQuotaPath delete path %s pod:%s is activing", path, info.PodId)
						continue
					}
				}
				err := os.RemoveAll(path)
				if err == nil {
					save = false
				}
				glog.Infof("CleanOrphanQuotaPath remove path %s ,err=%v", path, err)
			} else if notRecycle == false { // the pod is not active and need recycle and clean the path
				if ok, info := xfsqm.IsPathXFSQuota(path); ok == true && activeFun(info.PodId, "") == true {
					glog.Infof("CleanOrphanQuotaPath recycle path %s pod:%s is activing", path, info.PodId)
					continue
				} else if xfsqm.runner.IsHostQuotaPathExist(path+xfsKeepForOnePodInnerDir) == true {
					err := os.RemoveAll(path + xfsKeepForOnePodInnerDir)
					glog.Infof("CleanOrphanQuotaPath recycle path %s ,err=%v", path, err)
				}
			}
			if save == true {
				activeDirNames[entry.Name()] = struct{}{}
			}
		}
		cleanOwnerIds(disk.MountPath, activeDirNames, activeFun, xfsqm.runner.ReadFile)
	}
	for path, _ := range xfsqm.xfsHistoryProjectList {
		path = getStandardPathString(path)
		if _, exists := activePaths[path]; exists == false {
			xfsqm.removeHistoryInfo(path)
		}
	}
	xfsqm.refreshProjectInfo()
	xfsqm.saveHistoryInfo()
	return nil
}

func (xfsqm *XFSQuotaManager) GetQuotaStatus() QuotaStatus {
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	return xfsqm.getQuotaStatus()
}
func (xfsqm *XFSQuotaManager) getQuotaStatus() QuotaStatus {
	return xfsqm.quotaStatus
}

func (xfsqm *XFSQuotaManager) syncQuotaStatus() (changeSize int64) {
	infos := xfsqm.GetQuotaDiskInfos()
	newQuotaStatus := QuotaStatus{
		DiskStatus: make([]DiskQuotaStatus, 0, len(infos)),
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
	for _, diskInfo := range infos {
		_, size := GetDirFileSize(diskInfo.MountPath)
		ds := DiskQuotaStatus{
			Capacity:      diskInfo.Capacity,
			CurUseSize:    size,
			CurQuotaSize:  diskInfo.QuotaedSize,
			AvaliableSize: fmt.Sprintf("%s, %s", funcSizeToStr(diskInfo.Capacity-size), funcSizeToStr(diskInfo.Capacity-diskInfo.QuotaedSize)),
			MountPath:     diskInfo.MountPath,
		}
		newQuotaStatus.Capacity += ds.Capacity
		newQuotaStatus.CurQuotaSize += ds.CurQuotaSize
		newQuotaStatus.CurUseSize += ds.CurUseSize
		newQuotaStatus.DiskStatus = append(newQuotaStatus.DiskStatus, ds)
		statusStr += fmt.Sprintf("%s %d %d %d\n", ds.MountPath, ds.Capacity, ds.CurUseSize, ds.CurQuotaSize)
	}
	xfsqm.runner.WriteFile(path.Join(xfsqm.checkPath, xfsStatusFileName), statusStr)
	newQuotaStatus.AvaliableSize = fmt.Sprintf("%s, %s", funcSizeToStr(newQuotaStatus.Capacity-newQuotaStatus.CurUseSize),
		funcSizeToStr(newQuotaStatus.Capacity-newQuotaStatus.CurQuotaSize))
	xfsqm.mu.Lock()
	defer xfsqm.mu.Unlock()
	oldUseSize := xfsqm.quotaStatus.CurUseSize
	curUseSize := newQuotaStatus.CurUseSize
	xfsqm.quotaStatus = newQuotaStatus
	if oldUseSize > curUseSize {
		return oldUseSize - curUseSize
	} else {
		return curUseSize - oldUseSize
	}
}
func getSyncDuration(changeSize int64) time.Duration {
	var minDuration time.Duration = 1 * time.Minute
	var maxDuration time.Duration = 3 * time.Minute
	var defaultDuration time.Duration = 2 * time.Minute
	var minCareChangeSize int64 = 10 * 1024 * 1024
	var maxCareChangeSize int64 = 100 * 1024 * 1024
	if changeSize <= minCareChangeSize { // 10MB
		return maxDuration
	} else if changeSize <= maxCareChangeSize {
		return defaultDuration
	} else {
		return minDuration
	}
}
func (xfsqm *XFSQuotaManager) quotaChanged() {
	xfsqm.quotaProjectChanged = true
}
func (xfsqm *XFSQuotaManager) StartQuotaStatusSync() bool {
	changeChan := make(chan struct{})
	go func() {
		var stop bool
		for {
			if stop == true {
				break
			}
			select {
			case <-time.After(3 * time.Second):
				if xfsqm.quotaProjectChanged == true {
					xfsqm.quotaProjectChanged = false
					changeChan <- struct{}{}
				}
			case <-xfsqm.quotaStatusSync:
				stop = true
			}
		}

	}()
	go func() {
		var stop bool
		for {
			if stop == true {
				break
			}
			changeSize := xfsqm.syncQuotaStatus()

			select {
			case <-changeChan:
			case <-time.After(getSyncDuration(changeSize)):
			case <-xfsqm.quotaStatusSync:
				stop = true
			}
		}
	}()
	return true
}
func (xfsqm *XFSQuotaManager) StopQuotaStatusSync() bool {
	close(xfsqm.quotaStatusSync)
	return true
}
func checkSystemSupportXFSQuota() (bool, error) {
	if runtime.GOOS == "windows" { // windows not support the feature
		return false, fmt.Errorf("windows not support disk quota")
	}
	return true, nil
}

func getSizeToStr(size int64) string {
	return fmt.Sprintf("%dm", size/1024/1024) // to MB
}

func getStrToSize(str string) int64 {
	if strings.HasSuffix(str, "m") == false {
		return -1
	}
	str = strings.Replace(str, "m", "", -1)
	size, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return -1
	}
	return size
}

// for example: /xfs/disk1/testdir return testdir
func getFileNameFromDir(p string) string {
	return path.Base(p)
}

// for example: /xfs/disk1/testdir return /xfs/disk1/testdir/
func getStandardPathString(p string) string {
	return path.Clean(p) + "/"
}

func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

var sema = make(chan struct{}, 20)

func walkDir(dir string, wg *sync.WaitGroup, fileSizes chan<- int64) {
	defer wg.Done()
	for _, entry := range dirents(dir) {
		if entry.IsDir() {
			wg.Add(1)
			subDir := filepath.Join(dir, entry.Name())
			go walkDir(subDir, wg, fileSizes)
		} else {
			fileSizes <- entry.Size()
		}
	}
}

//read volume file info
func dirents(dir string) []os.FileInfo {
	sema <- struct{}{}
	defer func() { <-sema }()
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil
	}
	return entries
}

func GetDirFileSize(path string) (fileNum, fileSize int64) {
	fileSizes := make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go walkDir(path, &wg, fileSizes)

	go func() {
		wg.Wait()
		close(fileSizes)
	}()
loop:
	for {
		select {
		case size, ok := <-fileSizes:
			if !ok {
				break loop
			}
			fileNum++
			fileSize += size
		}
	}
	return fileNum, fileSize
}
