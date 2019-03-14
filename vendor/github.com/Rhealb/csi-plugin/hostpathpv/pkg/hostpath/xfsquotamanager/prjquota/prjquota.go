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

package prjquota

import (
	"fmt"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager/common"

	glog "k8s.io/klog"
)

const (
	cmdXFSQuota string = "xfs_quota"
	//cmdMkQuotaDir       string = "mkdir"
	cmdWhichXFSQuota    string = "which"
	xfsProjectsPath     string = "/etc/projects"
	xfsProjidPath       string = "/etc/projid"
	xfsHistoryQuotaPath string = "/etc/historyprojects"
	xfsMinQuota         int64  = 1 // 1MB
)

type XFSMountPath string
type XFSDevice string
type XFSProjectName string
type XFSProjectPath string
type XFSProjectID int64
type XFSDisk int64

type XFSProjectInfo struct {
	ProjectId XFSProjectID
	Path      XFSProjectPath
}

type XFSProjectInfoList []XFSProjectInfo

func (xpil *XFSProjectInfoList) Equal(other XFSProjectInfoList) bool {
	if len(*xpil) != len(other) {
		return false
	}
	for i := range *xpil {
		if (*xpil)[i].ProjectId != other[i].ProjectId || (*xpil)[i].Path != other[i].Path {
			return false
		}
	}
	return true
}
func (xpil *XFSProjectInfoList) Add(proid XFSProjectID, quotapath XFSProjectPath) error {
	for _, item := range *xpil {
		if item.ProjectId == proid {
			if path.Clean(string(quotapath)) == path.Clean(string(item.Path)) {
				return nil
			} else {
				return fmt.Errorf("projectid %d is used by quotapath %s", proid, item.Path)
			}
		} else if path.Clean(string(quotapath)) == path.Clean(string(item.Path)) {
			if item.ProjectId == proid {
				return nil
			} else {
				return fmt.Errorf("quotapath %s is added", quotapath)
			}
		}
	}
	*xpil = append(*xpil, XFSProjectInfo{ProjectId: proid, Path: quotapath})
	return nil
}

func (xpil *XFSProjectInfoList) GetPathById(proid XFSProjectID) XFSProjectPath {
	for _, item := range *xpil {
		if item.ProjectId == proid {
			return item.Path
		}
	}
	return XFSProjectPath("")
}

func (xpil *XFSProjectInfoList) DeleteById(proid XFSProjectID) {
	for i, info := range *xpil {
		if info.ProjectId == proid {
			*xpil = append((*xpil)[0:i], (*xpil)[i+1:]...)
			break
		}
	}
}
func (xpil *XFSProjectInfoList) DeleteByPath(path XFSProjectPath) {
	for i, info := range *xpil {
		if info.Path == path {
			*xpil = append((*xpil)[0:i], (*xpil)[i+1:]...)
			break
		}
	}
}
func (xpil *XFSProjectInfoList) ToString() string {
	ret := ""
	for _, info := range *xpil {
		ret += fmt.Sprintf("%d:%s\n", info.ProjectId, info.Path)
	}
	return ret
}
func (xpil *XFSProjectInfoList) Clone() XFSProjectInfoList {
	ret := make(XFSProjectInfoList, 0, len(*xpil))
	for _, info := range *xpil {
		ret = append(ret, info)
	}
	return ret
}

type XFSProjIdInfo struct {
	ProjectName XFSProjectName
	ProjectId   XFSProjectID
}
type XFSProjIdInfoList []XFSProjIdInfo

func (xpiil *XFSProjIdInfoList) Equal(other XFSProjIdInfoList) bool {
	if len(*xpiil) != len(other) {
		return false
	}
	for i := range *xpiil {
		if (*xpiil)[i].ProjectName != other[i].ProjectName || (*xpiil)[i].ProjectId != other[i].ProjectId {
			return false
		}
	}
	return true
}

func (xpiil *XFSProjIdInfoList) Add(proid XFSProjectID, proName XFSProjectName) error {
	for _, item := range *xpiil {
		if item.ProjectId == proid {
			if item.ProjectName == proName {
				return nil
			} else {
				return fmt.Errorf("project id %d is used by projectname %s", proid, item.ProjectName)
			}
		} else if item.ProjectName == proName {
			if item.ProjectId == proid {
				return nil
			} else {
				return fmt.Errorf("projectname %s is added", proName)
			}
		}
	}
	*xpiil = append(*xpiil, XFSProjIdInfo{ProjectId: proid, ProjectName: proName})
	return nil
}

func (xpiil *XFSProjIdInfoList) GetProjNameById(proid XFSProjectID) XFSProjectName {
	for _, item := range *xpiil {
		if item.ProjectId == proid {
			return item.ProjectName
		}
	}
	return XFSProjectName("")
}

func (xpiil *XFSProjIdInfoList) DeleteById(proid XFSProjectID) {
	for i, info := range *xpiil {
		if info.ProjectId == proid {
			*xpiil = append((*xpiil)[0:i], (*xpiil)[i+1:]...)
			return
		}
	}
}

func (xpiil *XFSProjIdInfoList) DeleteByName(proName XFSProjectName) {
	for i, info := range *xpiil {
		if info.ProjectName == proName {
			*xpiil = append((*xpiil)[0:i], (*xpiil)[i+1:]...)
			return
		}
	}
}

func (xpiil *XFSProjIdInfoList) ToString() string {
	ret := ""
	for _, info := range *xpiil {
		ret += fmt.Sprintf("%s:%d\n", info.ProjectName, info.ProjectId)
	}
	return ret
}
func (xpil *XFSProjIdInfoList) Clone() XFSProjIdInfoList {
	ret := make(XFSProjIdInfoList, 0, len(*xpil))
	for _, info := range *xpil {
		ret = append(ret, info)
	}
	return ret
}

type XFSStateInfo struct {
	MountPath                 XFSMountPath
	Device                    XFSDevice
	UserQuotaAccountingOn     bool
	UserQuotaEnforcementOn    bool
	GroupQuotaAccountingOn    bool
	GroupQuotaEnforcementOn   bool
	ProjectQuotaAccountingOn  bool
	ProjectQuotaEnforcementOn bool
}

type XFSReportInfo struct {
	ProjectName XFSProjectName
	Used        XFSDisk
	Soft        XFSDisk
	Hard        XFSDisk
}
type XFSProjectReportMap map[XFSProjectName]XFSReportInfo

type XFSDfInfo struct {
	Device    XFSDevice
	MountPath XFSMountPath
	Size      int64
	Used      int64
}
type XFSDfInfoList []XFSDfInfo

type Interface interface {
	SystemSupport() (bool, error)
	QuotaState() ([]XFSStateInfo, error)
	QuotaReport(paths []string, prefix string) (XFSProjectReportMap, error)
	QuotaProjects() XFSProjectInfoList
	QuotaProjids() XFSProjIdInfoList
	QuotaSetupProject(project string, paths []string) ([]byte, error)
	QuotaDfInfo(paths []string, pathPrefix string) (XFSDfInfoList, error)
	QuotaAddProjectInfo(projectId int64, projectName, quotaPath string) error
	QuotaDeleteProjectInfo(projectId int64) error
	LimitQuotaProject(project string, softLimit, hardLimit int64) ([]byte, int64, int64, error)
}

type prjquota struct {
	quotaRootPath                 string
	etcfileMu                     sync.RWMutex
	host                          HostInterface
	maxCacheTime                  time.Duration
	cacheReportMap                XFSProjectReportMap
	cacheReportMapTime            time.Time
	cacheReportXFSProjectInfoList XFSProjectInfoList
	cacheDfList                   XFSDfInfoList
	cacheDfListTime               time.Time
	cacheDfXFSProjectInfoList     XFSProjectInfoList
	cacheStates                   []XFSStateInfo
	cascheQuotaDiskPathStrs       string
	cacheStatesTime               time.Time
}

func New(host HostInterface, rootPath string) Interface {
	return &prjquota{
		maxCacheTime:  10 * time.Second,
		host:          host,
		quotaRootPath: rootPath,
	}
}

// whether xfs_quota is installed
func (rq *prjquota) SystemSupport() (bool, error) {
	bytes, err := exec.Command(cmdWhichXFSQuota, cmdXFSQuota).CombinedOutput()
	if err != nil {
		return false, err
	}
	helpMatcher := regexp.MustCompile("(/.+)")
	if helpMatcher.MatchString(string(bytes)) == true {
		return true, nil
	} else {
		return false, fmt.Errorf("xfs_quota error CombinedOutput=%s", string(bytes))
	}
}

/* get the quota state by `xfs_quota -x -c state`
output as follows:
User quota state on /xfs/disk1 (/dev/sdb1)
  Accounting: OFF
  Enforcement: OFF
  Inode: N/A
Group quota state on /xfs/disk1 (/dev/sdb1)
  Accounting: OFF
  Enforcement: OFF
  Inode: N/A
Project quota state on /xfs/disk1 (/dev/sdb1)
  Accounting: ON
  Enforcement: ON
  Inode: #99 (7 blocks, 5 extents)
Blocks grace time: [7 days]
Inodes grace time: [7 days]
Realtime Blocks grace time: [7 days]*/
func (rq *prjquota) QuotaState() ([]XFSStateInfo, error) {
	dirs := strings.Join(rq.host.GetSubDirs(rq.quotaRootPath), ",")
	if rq.cascheQuotaDiskPathStrs != "" && rq.cacheStates != nil &&
		rq.cascheQuotaDiskPathStrs == dirs && time.Since(rq.cacheStatesTime) < rq.maxCacheTime {
		return rq.cacheStates, nil
	}

	output, err := exec.Command(cmdXFSQuota, []string{"-x", "-c", "state"}...).CombinedOutput()
	if err != nil {
		return []XFSStateInfo{}, fmt.Errorf("run %s -x -c state err:%v", cmdXFSQuota, err)
	}
	ret := decodeStateInfo(output)
	rq.cacheStatesTime = time.Now()
	rq.cascheQuotaDiskPathStrs = dirs
	rq.cacheStates = ret
	return ret, nil
}

/*
`xfs_quota -x -c report /xfs/disk1`
output as follows:
Project quota on /xfs/disk1 (/dev/sdb1)
                               Blocks
Project ID       Used       Soft       Hard    Warn/Grace
---------- --------------------------------------------------
k8spro1        399984   20971520   20971520     00 [--------]
k8spro2             0      51200      51200     00 [--------]
*/
func (rq *prjquota) QuotaReport(paths []string, prefix string) (XFSProjectReportMap, error) {
	list := rq.QuotaProjects()
	if rq.cacheReportXFSProjectInfoList != nil && rq.cacheReportMap != nil &&
		rq.cacheReportXFSProjectInfoList.Equal(list) && time.Since(rq.cacheReportMapTime) < rq.maxCacheTime {
		return rq.cacheReportMap, nil
	}
	args := []string{"-x", "-c", "report -pb"}
	if paths != nil {
		args = append(args, paths...)
	}
	output, err := exec.Command(cmdXFSQuota, args...).CombinedOutput()
	if err != nil {
		return make(XFSProjectReportMap), fmt.Errorf("%s %s err:%v", cmdXFSQuota, strings.Join(args, " "), err)
	}
	ret := decodeReportInfo(output, prefix)
	rq.cacheReportMapTime = time.Now()
	rq.cacheReportXFSProjectInfoList = list
	rq.cacheReportMap = ret

	return ret, nil
}

func (rq *prjquota) QuotaProjects() XFSProjectInfoList {
	content := rq.host.ReadFile(xfsProjectsPath)
	return decodeProjectInfo([]byte(content))
}

func (rq *prjquota) QuotaProjids() XFSProjIdInfoList {
	content := rq.host.ReadFile(xfsProjidPath)
	return decodeProjIdInfo([]byte(content))
}

func (rq *prjquota) QuotaSetupProject(project string, paths []string) ([]byte, error) {
	args := []string{"-x", "-c", fmt.Sprintf("project -s %s", project)}
	if paths != nil {
		args = append(args, paths...)
	}
	cmd := exec.Command(cmdXFSQuota, args...)
	stop := make(chan struct{}, 0)
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10000 * time.Millisecond):
				if cmd.Process != nil {
					cmd.Process.Kill()
					glog.Errorf("'xfs_quota -x -c project -s %s' timeout kill", project)
				}
				glog.Errorf("'xfs_quota -x -c project -s %s' 10s timeout", project)
			}
		}
	}()
	return cmd.CombinedOutput()
}

func (rq *prjquota) QuotaDfInfo(paths []string, pathPrefix string) (XFSDfInfoList, error) {
	list := rq.QuotaProjects()
	if rq.cacheDfXFSProjectInfoList != nil && rq.cacheDfList != nil &&
		rq.cacheDfXFSProjectInfoList.Equal(list) && time.Since(rq.cacheDfListTime) < rq.maxCacheTime {
		return rq.cacheDfList, nil
	}

	args := []string{"-x", "-c", "df "}
	if paths != nil {
		args = append(args, paths...)
	}
	output, err := exec.Command(cmdXFSQuota, args...).CombinedOutput()
	if err != nil {
		//		return XFSDfInfoList{}, fmt.Errorf("%s -x -c df err:%v", cmdXFSQuota, err)
		//		glog.Errorf("xfs_quota df err:%v\n", err)
	}
	ret := decodeUsedInfo(output, pathPrefix)
	rq.cacheDfListTime = time.Now()
	rq.cacheDfXFSProjectInfoList = list
	rq.cacheDfList = ret

	return ret, nil
}

func (rq *prjquota) LimitQuotaProject(project string, softLimit, hardLimit int64) ([]byte, int64, int64, error) {
	softLimit = softLimit / 1024 / 1024
	if softLimit < xfsMinQuota {
		softLimit = xfsMinQuota
	}
	hardLimit = hardLimit / 1024 / 1024
	if hardLimit < xfsMinQuota {
		hardLimit = xfsMinQuota
	}

	buf, _ := exec.Command(cmdXFSQuota, []string{"-x", "-c", fmt.Sprintf("limit -p bsoft=%dM bhard=%dM %s",
		softLimit, hardLimit, project)}...).CombinedOutput()
	return buf, softLimit * 1024 * 1024, hardLimit * 1024 * 1024, nil
}

func (rq *prjquota) quotaPathCheck(quotaPath string) error {
	if ok, err := rq.SystemSupport(); ok == false || err != nil {
		return fmt.Errorf("xfs_quota is not support err:%v", err)
	}
	if rq.host.IsPathExist(quotaPath) == false {
		return fmt.Errorf("quotapath %s is not exist", quotaPath)
	}
	states, err := rq.QuotaState()
	if err != nil {
		return fmt.Errorf("get quota state err:%v", err)
	}
	for _, s := range states {
		if s.ProjectQuotaEnforcementOn == true {
			mp := path.Clean(string(s.MountPath))
			if strings.HasPrefix(quotaPath, mp) {
				return nil
			}
		}
	}
	return fmt.Errorf("%s is not a valid quotapath", quotaPath)
}
func (rq *prjquota) QuotaAddProjectInfo(projectId int64, projectName, quotaPath string) error {
	if errCheck := rq.quotaPathCheck(quotaPath); errCheck != nil {
		return errCheck
	}
	rq.etcfileMu.Lock()
	defer rq.etcfileMu.Unlock()

	projects := rq.QuotaProjects()
	projIds := rq.QuotaProjids()

	projectsClone := projects.Clone()

	if errAdd := projects.Add(XFSProjectID(projectId), XFSProjectPath(quotaPath)); errAdd != nil {
		return fmt.Errorf("QuotaAddProjectInfo err:%v", errAdd)
	}
	if errAdd := projIds.Add(XFSProjectID(projectId), XFSProjectName(projectName)); errAdd != nil {
		return fmt.Errorf("QuotaAddProjectInfo err:%v", errAdd)
	}

	if err := rq.host.WriteFile(xfsProjectsPath, projects.ToString()); err != nil {
		return fmt.Errorf("QuotaAddProjectInfo write file %s err:%v", xfsProjectsPath, err)
	}
	if err := rq.host.WriteFile(xfsProjidPath, projIds.ToString()); err != nil {
		errRollback := rq.host.WriteFile(xfsProjectsPath, projectsClone.ToString())
		return fmt.Errorf("QuotaAddProjectInfo write file %s err:%v, errRollback:%v", xfsProjidPath, err, errRollback)
	}
	return nil
}

func (rq *prjquota) QuotaDeleteProjectInfo(projectId int64) error {
	rq.etcfileMu.Lock()
	defer rq.etcfileMu.Unlock()
	projects := rq.QuotaProjects()
	projIds := rq.QuotaProjids()

	projectsClone := projects.Clone()

	projects.DeleteById(XFSProjectID(projectId))
	projIds.DeleteById(XFSProjectID(projectId))

	if err := rq.host.WriteFile(xfsProjectsPath, projects.ToString()); err != nil {
		return fmt.Errorf("QuotaDeleteProjectInfo write file %s err:%v", xfsProjectsPath, err)
	}
	if err := rq.host.WriteFile(xfsProjidPath, projIds.ToString()); err != nil {
		errRollback := rq.host.WriteFile(xfsProjectsPath, projectsClone.ToString())
		return fmt.Errorf("QuotaDeleteProjectInfo write file %s err:%v, errRollback:%v", xfsProjidPath, err, errRollback)
	}
	return nil
}

func decodeUsedInfo(info []byte, pathPrefix string) XFSDfInfoList {
	ret := make(XFSDfInfoList, 0)
	readIndex := 0
	// find beginning of table
	mountedPath := make(map[string]bool)
	for readIndex < len(info) {
		line, n := readLine(readIndex, info)

		readIndex = n
		if len(line) == 0 {
			continue
		}
		// skip error and head info
		if strings.Contains(line, "Filesystem") == false && strings.Contains(line, "xfs_quota: project quota flag not set on") == false {
			strs := mySplit(line)
			if len(strs) == 6 {
				size, err1 := strconv.ParseInt(strs[1], 10, 64)
				used, err2 := strconv.ParseInt(strs[2], 10, 64)
				if err1 == nil && err2 == nil {
					if _, exists := mountedPath[strs[5]]; exists == false {
						if pathPrefix == "" || strings.HasPrefix(strs[5], pathPrefix) {
							ret = append(ret, XFSDfInfo{
								Device:    XFSDevice(strs[0]),
								MountPath: XFSMountPath(strs[5]),
								Size:      size,
								Used:      used,
							})
						}
						mountedPath[strs[5]] = true
					}
				}
			}
		}
	}
	return ret
}

func decodeProjIdInfo(info []byte) XFSProjIdInfoList {
	proidInfo := make(XFSProjIdInfoList, 0)
	readIndex := 0
	// find beginning of table
	for readIndex < len(info) {
		line, n := readLine(readIndex, info)

		readIndex = n
		if len(line) == 0 {
			continue
		}
		if strings.Contains(line, ":") {
			strs := strings.Split(line, ":")
			if len(strs) == 2 {
				id, err := strconv.ParseInt(strs[1], 10, 64)
				if err == nil {
					proidInfo.Add(XFSProjectID(id), XFSProjectName(strs[0]))
				}
			}
		}
	}
	return proidInfo
}

func decodeReportInfo(info []byte, prefix string) XFSProjectReportMap {
	ret := make(XFSProjectReportMap)
	readIndex := 0
	// find beginning of table
	started := false
	for readIndex < len(info) {
		line, n := readLine(readIndex, info)

		readIndex = n
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		if started != true {
			if isReportStart(line) == true {
				started = true
				continue
			} else {
				continue
			}
		}
		strs := mySplit(line)
		if len(strs) == 6 {
			used, err1 := strconv.ParseInt(strs[1], 10, 64)
			soft, err2 := strconv.ParseInt(strs[2], 10, 64)
			hard, err3 := strconv.ParseInt(strs[3], 10, 64)
			if err1 == nil && err2 == nil && err3 == nil {
				if prefix == "" || strings.HasPrefix(strs[0], prefix) {
					ret[XFSProjectName(strs[0])] = XFSReportInfo{
						ProjectName: XFSProjectName(strs[0]),
						Used:        XFSDisk(used),
						Soft:        XFSDisk(soft),
						Hard:        XFSDisk(hard),
					}
				}
			}
		}
	}
	return ret
}

func readLine(readIndex int, byteArray []byte) (string, int) {
	currentReadIndex := readIndex

	// consume left spaces
	for currentReadIndex < len(byteArray) {
		if byteArray[currentReadIndex] == ' ' {
			currentReadIndex++
		} else {
			break
		}
	}

	// leftTrimIndex stores the left index of the line after the line is left-trimmed
	leftTrimIndex := currentReadIndex

	// rightTrimIndex stores the right index of the line after the line is right-trimmed
	// it is set to -1 since the correct value has not yet been determined.
	rightTrimIndex := -1

	for ; currentReadIndex < len(byteArray); currentReadIndex++ {
		if byteArray[currentReadIndex] == ' ' {
			// set rightTrimIndex
			if rightTrimIndex == -1 {
				rightTrimIndex = currentReadIndex
			}
		} else if (byteArray[currentReadIndex] == '\n') || (currentReadIndex == (len(byteArray) - 1)) {
			// end of line or byte buffer is reached
			if currentReadIndex <= leftTrimIndex {
				return "", currentReadIndex + 1
			}
			// set the rightTrimIndex
			if rightTrimIndex == -1 {
				rightTrimIndex = currentReadIndex
				if currentReadIndex == (len(byteArray)-1) && (byteArray[currentReadIndex] != '\n') {
					// ensure that the last character is part of the returned string,
					// unless the last character is '\n'
					rightTrimIndex = currentReadIndex + 1
				}
			}
			return string(byteArray[leftTrimIndex:rightTrimIndex]), currentReadIndex + 1
		} else {
			// unset rightTrimIndex
			rightTrimIndex = -1
		}
	}
	return "", currentReadIndex
}

func decodeStateInfo(info []byte) []XFSStateInfo {
	ret := make([]XFSStateInfo, 0)
	readIndex := 0
	// find beginning of table
	xfsStateinfo := XFSStateInfo{}

	lastMountPath := ""
	lastQuotaType := -1
	setFun := func(setinfo *XFSStateInfo, t int, isAccounting, isOn bool) {
		switch t {
		case 1:
			if isAccounting {
				setinfo.UserQuotaAccountingOn = isOn
			} else {
				setinfo.UserQuotaEnforcementOn = isOn
			}
		case 2:
			if isAccounting {
				setinfo.GroupQuotaAccountingOn = isOn
			} else {
				setinfo.GroupQuotaEnforcementOn = isOn
			}
		case 3:
			if isAccounting {
				setinfo.ProjectQuotaAccountingOn = isOn
			} else {
				setinfo.ProjectQuotaEnforcementOn = isOn
			}
		}
	}
	for readIndex < len(info) {
		line, n := readLine(readIndex, info)
		readIndex = n
		if len(line) == 0 {
			continue
		}
		if strings.HasPrefix(line, "User quota state on") {
			strs := strings.Split(line, " ")
			if len(strs) == 6 {
				if lastMountPath != strs[4] {
					if lastMountPath != "" {
						ret = append(ret, xfsStateinfo)
					}
					xfsStateinfo = XFSStateInfo{}
					lastMountPath = strs[4]
				}
				xfsStateinfo.MountPath = XFSMountPath(strs[4])
				xfsStateinfo.Device = XFSDevice(strs[5][1 : len(strs[5])-1])
				lastQuotaType = 1
			}
		} else if strings.HasPrefix(line, "Group quota state on") {
			strs := strings.Split(line, " ")
			if len(strs) == 6 {
				if lastMountPath != strs[4] {
					if lastMountPath != "" {
						ret = append(ret, xfsStateinfo)
					}
					xfsStateinfo = XFSStateInfo{}
					lastMountPath = strs[4]
				}
				xfsStateinfo.MountPath = XFSMountPath(strs[4])
				xfsStateinfo.Device = XFSDevice(strs[5][1 : len(strs[5])-1])
				lastQuotaType = 2
			}
		} else if strings.HasPrefix(line, "Project quota state on") {
			strs := strings.Split(line, " ")
			if len(strs) == 6 {
				if lastMountPath != strs[4] {
					if lastMountPath != "" {
						ret = append(ret, xfsStateinfo)
					}
					xfsStateinfo = XFSStateInfo{}
					lastMountPath = strs[4]
				}
				xfsStateinfo.MountPath = XFSMountPath(strs[4])
				xfsStateinfo.Device = XFSDevice(strs[5][1 : len(strs[5])-1])
				lastQuotaType = 3
			}
		} else if strings.Contains(line, "Accounting") {
			isOn := true
			if strings.Contains(line, "OFF") {
				isOn = false
			}
			setFun(&xfsStateinfo, lastQuotaType, true, isOn)
		} else if strings.Contains(line, "Enforcement") {
			isOn := true
			if strings.Contains(line, "OFF") {
				isOn = false
			}
			setFun(&xfsStateinfo, lastQuotaType, false, isOn)
		}
	}
	if lastMountPath != "" {
		ret = append(ret, xfsStateinfo)
	}

	filterRet := make([]XFSStateInfo, 0, len(ret))
	addedPath := make(map[string]bool)
	for _, info := range ret {
		if addedPath[string(info.MountPath)] == false {
			addedPath[string(info.MountPath)] = true
			filterRet = append(filterRet, info)
		}
	}
	return filterRet
}

func decodeProjectInfo(info []byte) XFSProjectInfoList {
	proInfo := make(XFSProjectInfoList, 0)
	readIndex := 0
	// find beginning of table
	for readIndex < len(info) {
		line, n := readLine(readIndex, info)

		readIndex = n
		if len(line) == 0 {
			continue
		}
		if strings.Contains(line, ":") {
			strs := strings.Split(line, ":")
			if len(strs) == 2 {
				id, err := strconv.ParseInt(strs[0], 10, 64)
				if err == nil {
					proInfo.Add(XFSProjectID(id), XFSProjectPath(strs[1]))
				}
			}
		}
	}
	return proInfo
}

func isReportStart(line string) bool {
	line = strings.Replace(line, " ", "", -1)
	if strings.Repeat("-", len(line)) == line {
		return true
	}
	return false
}
func mySplit(line string) []string {
	strs := strings.Split(line, " ")
	ret := make([]string, 0, len(strs))
	for _, str := range strs {
		if str != "" {
			ret = append(ret, str)
		}
	}
	return ret
}

type fakePrjquota struct {
	xfsStateInfo   []XFSStateInfo
	pathQuotaInfos []FakePathQuotaInfo
	host           HostInterface
	disksizemap    map[string]int64
}
type FakePathQuotaInfo struct {
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

func NewFake(host HostInterface, xfsStateInfo []XFSStateInfo, disksizemap map[string]int64, pathQuotaInfos []FakePathQuotaInfo) Interface {
	ret := &fakePrjquota{
		host:           host,
		xfsStateInfo:   xfsStateInfo,
		pathQuotaInfos: pathQuotaInfos,
		disksizemap:    disksizemap,
	}
	for _, info := range pathQuotaInfos {
		ret.QuotaAddProjectInfo(info.ProjectId, info.ProjectName, info.Path)
		common.SetOwnerId(info.Path, info.OwnerId, host.WriteFile)
		if info.IsCSIQuotaPath == true {
			host.WriteFile(path.Join(info.Path, common.XfsCSIFlagFile), "true")
		}
		host.MkDir(info.Path)
	}
	return ret
}
func (fakePrjquota *fakePrjquota) SystemSupport() (bool, error) {
	return true, nil
}
func (fakePrjquota *fakePrjquota) QuotaState() ([]XFSStateInfo, error) {
	return fakePrjquota.xfsStateInfo, nil
}
func (fakePrjquota *fakePrjquota) QuotaReport(paths []string, prefix string) (XFSProjectReportMap, error) {
	ret := make(XFSProjectReportMap)
	for _, info := range fakePrjquota.pathQuotaInfos {
		ret[XFSProjectName(info.ProjectName)] = XFSReportInfo{
			ProjectName: XFSProjectName(info.ProjectName),
			Used:        XFSDisk(info.UsedSize),
			Soft:        XFSDisk(info.SoftQuota),
			Hard:        XFSDisk(info.HardQuota),
		}
	}
	return ret, nil
}

func (fakePrjquota *fakePrjquota) QuotaProjects() XFSProjectInfoList {
	content := fakePrjquota.host.ReadFile(xfsProjectsPath)
	return decodeProjectInfo([]byte(content))
}

func (fakePrjquota *fakePrjquota) QuotaProjids() XFSProjIdInfoList {
	content := fakePrjquota.host.ReadFile(xfsProjidPath)
	return decodeProjIdInfo([]byte(content))
}
func (fakePrjquota *fakePrjquota) QuotaSetupProject(project string, paths []string) ([]byte, error) {
	return []byte{}, nil
}
func (fakePrjquota *fakePrjquota) QuotaDfInfo(paths []string, pathPrefix string) (XFSDfInfoList, error) {
	ret := make(XFSDfInfoList, 0, len(fakePrjquota.pathQuotaInfos))
	for _, disk := range fakePrjquota.xfsStateInfo {
		var usedsize int64
		for _, info := range fakePrjquota.pathQuotaInfos {
			if strings.HasPrefix(info.Path, string(disk.MountPath)) {
				usedsize += info.UsedSize
			}
		}
		if strings.HasPrefix(string(disk.MountPath), pathPrefix) {
			ret = append(ret, XFSDfInfo{
				Device:    disk.Device,
				MountPath: disk.MountPath,
				Size:      fakePrjquota.disksizemap[string(disk.Device)],
				Used:      usedsize,
			})
		}
	}
	for _, info := range fakePrjquota.pathQuotaInfos {
		if strings.HasPrefix(info.Path, pathPrefix) {
			ret = append(ret, XFSDfInfo{
				Device:    XFSDevice(""),
				MountPath: XFSMountPath(info.Path),
				Size:      info.HardQuota,
				Used:      info.UsedSize,
			})
		}
	}
	return ret, nil
}
func (fakePrjquota *fakePrjquota) QuotaAddProjectInfo(projectId int64, projectName, quotaPath string) error {
	projects := fakePrjquota.QuotaProjects()
	projIds := fakePrjquota.QuotaProjids()
	projectsClone := projects.Clone()
	if errAdd := projects.Add(XFSProjectID(projectId), XFSProjectPath(quotaPath)); errAdd != nil {
		return fmt.Errorf("QuotaAddProjectInfo err:%v", errAdd)
	}
	if errAdd := projIds.Add(XFSProjectID(projectId), XFSProjectName(projectName)); errAdd != nil {
		return fmt.Errorf("QuotaAddProjectInfo err:%v", errAdd)
	}

	if err := fakePrjquota.host.WriteFile(xfsProjectsPath, projects.ToString()); err != nil {
		return fmt.Errorf("QuotaAddProjectInfo write file %s err:%v", xfsProjectsPath, err)
	}
	if err := fakePrjquota.host.WriteFile(xfsProjidPath, projIds.ToString()); err != nil {
		errRollback := fakePrjquota.host.WriteFile(xfsProjectsPath, projectsClone.ToString())
		return fmt.Errorf("QuotaAddProjectInfo write file %s err:%v, errRollback:%v", xfsProjidPath, err, errRollback)
	}
	return nil
}

func (fakePrjquota *fakePrjquota) QuotaDeleteProjectInfo(projectId int64) error {
	projects := fakePrjquota.QuotaProjects()
	projIds := fakePrjquota.QuotaProjids()

	projectsClone := projects.Clone()

	projects.DeleteById(XFSProjectID(projectId))
	projIds.DeleteById(XFSProjectID(projectId))

	if err := fakePrjquota.host.WriteFile(xfsProjectsPath, projects.ToString()); err != nil {
		return fmt.Errorf("QuotaDeleteProjectInfo write file %s err:%v", xfsProjectsPath, err)
	}
	if err := fakePrjquota.host.WriteFile(xfsProjidPath, projIds.ToString()); err != nil {
		errRollback := fakePrjquota.host.WriteFile(xfsProjectsPath, projectsClone.ToString())
		return fmt.Errorf("QuotaDeleteProjectInfo write file %s err:%v, errRollback:%v", xfsProjidPath, err, errRollback)
	}
	return nil
}
func (fakePrjquota *fakePrjquota) LimitQuotaProject(project string, softLimit, hardLimit int64) ([]byte, int64, int64, error) {
	projids := fakePrjquota.QuotaProjids()
	projects := fakePrjquota.QuotaProjects()
	var projectId int64 = -1
	for _, pi := range projids {
		if string(pi.ProjectName) == project {
			projectId = int64(pi.ProjectId)
			break
		}
	}
	if projectId < 0 {
		return []byte{}, 0, 0, fmt.Errorf("cann't find project %s projectid", project)
	}
	projPath := ""
	for _, p := range projects {
		if int64(p.ProjectId) == projectId {
			projPath = string(p.Path)
			break
		}
	}
	if projPath == "" {
		return []byte{}, 0, 0, fmt.Errorf("cann't find project %s path", project)
	}
	volumeId, podId, ownerId, isKeep, isShare, errDirInfo := common.GetInfoFromDirName(projPath, fakePrjquota.host.ReadFile)
	if errDirInfo != nil {
		return []byte{}, 0, 0, errDirInfo
	}
	fakePrjquota.pathQuotaInfos = append(fakePrjquota.pathQuotaInfos, FakePathQuotaInfo{
		Path:        projPath,
		VolumeId:    volumeId,
		PodId:       podId,
		OwnerId:     ownerId,
		ProjectId:   projectId,
		ProjectName: project,
		UsedSize:    0,
		SoftQuota:   softLimit,
		HardQuota:   hardLimit,
		IsKeep:      isKeep,
		IsShare:     isShare,
	})
	return []byte{}, softLimit, hardLimit, nil
}
