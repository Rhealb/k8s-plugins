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

package utils

import (
	"fmt"
	"path"
	"strings"
)

type FakeXFSDevice struct {
	Device       string
	MountPath    string
	UserQuotaOn  bool
	GroupQuotaOn bool
	ProjQuotaOn  bool
	Capacity     int64
}
type FakeXFSQuotaProject struct {
	MountPath   string
	SoftQuota   int64
	HardQuota   int64
	UsedSize    int64
	ProjectName string
	ProjectId   int64
	Id          string
	KeepId      string
	SubId       string
	OwnerId     string
}

type FakeXFSQuotaRunner struct {
	projectFile string
	projidFile  string
	history     string
	devices     []FakeXFSDevice
	projects    []FakeXFSQuotaProject
	hostpaths   []string
	files       map[string]string
}

func NewFakeXFSQuotaRunner(devices []FakeXFSDevice, projects []FakeXFSQuotaProject, hostpaths []string, historyInfo string) Interface {
	projectFile := ""
	projidFile := ""
	for _, proj := range projects {
		projectFile += fmt.Sprintf("%d:%s\n", proj.ProjectId, proj.MountPath)
		projidFile += fmt.Sprintf("%s:%d\n", proj.ProjectName, proj.ProjectId)
	}
	return &FakeXFSQuotaRunner{
		devices:     devices,
		projects:    projects,
		projectFile: projectFile,
		projidFile:  projidFile,
		hostpaths:   hostpaths,
		history:     historyInfo,
		files:       make(map[string]string),
	}
}
func (fxfsqr *FakeXFSQuotaRunner) CurSystemSupport() (bool, error) {
	return true, nil
}
func getOnOffStr(ison bool) string {
	if ison {
		return "ON"
	} else {
		return "OFF"
	}
}
func (fxfsqr *FakeXFSQuotaRunner) QuotaState() ([]byte, error) {
	ret := ""
	for _, device := range fxfsqr.devices {
		ret += fmt.Sprintf("User quota state on %s (%s)\n", device.MountPath, device.Device)
		ret += fmt.Sprintf("  Accounting: %s\n", getOnOffStr(device.UserQuotaOn))
		ret += fmt.Sprintf("  Enforcement: %s\n", getOnOffStr(device.UserQuotaOn))
		ret += fmt.Sprintf("  Inode: #0 (0 blocks, 0 extents)\n")

		ret += fmt.Sprintf("Group quota state on %s (%s)\n", device.MountPath, device.Device)
		ret += fmt.Sprintf("  Accounting: %s\n", getOnOffStr(device.GroupQuotaOn))
		ret += fmt.Sprintf("  Enforcement: %s\n", getOnOffStr(device.GroupQuotaOn))
		ret += fmt.Sprintf("  Inode: #0 (0 blocks, 0 extents)\n")

		ret += fmt.Sprintf("Project quota state on %s (%s)\n", device.MountPath, device.Device)
		ret += fmt.Sprintf("  Accounting: %s\n", getOnOffStr(device.ProjQuotaOn))
		ret += fmt.Sprintf("  Enforcement: %s\n", getOnOffStr(device.ProjQuotaOn))
		ret += fmt.Sprintf("  Inode: #0 (0 blocks, 0 extents)\n")

		ret += fmt.Sprintf("Blocks grace time: [7 days 00:00:30]\n")
		ret += fmt.Sprintf("Inodes grace time: [7 days 00:00:30]\n")
		ret += fmt.Sprintf("Realtime Blocks grace time: [7 days 00:00:30]\n")
	}
	return []byte(ret), nil
}

func (fxfsqr *FakeXFSQuotaRunner) getDeviceByPath(path string) string {
	for _, device := range fxfsqr.devices {
		if strings.HasPrefix(path, device.MountPath) {
			return device.Device
		}
	}
	return ""
}
func (fxfsqr *FakeXFSQuotaRunner) QuotaPrint(paths []string) ([]byte, error) {
	ret := "Filesystem          Pathname\n"
	for _, device := range fxfsqr.devices {
		ret += fmt.Sprintf("%s          %s (pquota)\n", device.MountPath, device.Device)
	}
	for _, quotaProject := range fxfsqr.projects {
		ret += fmt.Sprintf("%s %s (project %d, %s)\n", quotaProject.MountPath,
			fxfsqr.getDeviceByPath(quotaProject.MountPath), quotaProject.ProjectId, quotaProject.ProjectName)
	}
	return []byte(ret), nil
}
func (fxfsqr *FakeXFSQuotaRunner) getDeviceUsedSize(mountPath string) int64 {
	var ret int64
	for _, project := range fxfsqr.projects {
		if strings.HasPrefix(project.MountPath, mountPath) {
			ret += project.UsedSize
		}
	}
	return ret
}
func (fxfsqr *FakeXFSQuotaRunner) QuotaUsedInfo(paths []string) ([]byte, error) {
	ret := "Filesystem           1K-blocks       Used  Available  Use% Pathname\n"
	for _, device := range fxfsqr.devices {
		used := fxfsqr.getDeviceUsedSize(device.MountPath)
		if used > device.Capacity {
			used = device.Capacity
		}
		ret += fmt.Sprintf("%s             %d     %d   %d    %d%% %s\n",
			device.Device, device.Capacity/1024, used/1024, (device.Capacity-used)/1024, used*100/device.Capacity, device.MountPath)
	}
	for _, quotaProject := range fxfsqr.projects {
		ret += fmt.Sprintf("%s               %d     %d          %d  %d%% %s\n",
			fxfsqr.getDeviceByPath(quotaProject.MountPath), quotaProject.HardQuota/1024, quotaProject.UsedSize/1024,
			(quotaProject.HardQuota-quotaProject.UsedSize)/1024,
			quotaProject.UsedSize*100/quotaProject.HardQuota, quotaProject.MountPath)
	}
	return []byte(ret), nil
}
func (fxfsqr *FakeXFSQuotaRunner) HistoryQuota() ([]byte, error) {
	return []byte(fxfsqr.history), nil
}
func (fxfsqr *FakeXFSQuotaRunner) QuotaProjects() ([]byte, error) {
	return []byte(fxfsqr.projectFile), nil
}
func (fxfsqr *FakeXFSQuotaRunner) QuotaProjid() ([]byte, error) {
	return []byte(fxfsqr.projidFile), nil
}
func (fxfsqr *FakeXFSQuotaRunner) QuotaReport(paths []string) ([]byte, error) {
	ret := "                               Blocks\n"
	ret += "Project ID       Used       Soft       Hard    Warn/Grace\n"
	ret += "---------- --------------------------------------------------\n"
	for _, quotaProject := range fxfsqr.projects {
		ret += fmt.Sprintf("%s        %d     %d     %d     00 [--------]\n",
			quotaProject.ProjectName, quotaProject.UsedSize/1024, quotaProject.SoftQuota/1024,
			quotaProject.HardQuota/1024)
	}
	return []byte(ret), nil
}
func (fxfsqr *FakeXFSQuotaRunner) SaveHistoryQuotaInfo(info string) error {
	fxfsqr.history = info
	return nil
}
func (fxfsqr *FakeXFSQuotaRunner) SaveQuotaProjectsAndId(projects, projectid string) error {
	fxfsqr.projectFile, fxfsqr.projidFile = projects, projectid
	return nil
}
func (fxfsqr *FakeXFSQuotaRunner) SetupQuotaProject(project string, paths []string) ([]byte, error) {
	return nil, nil
}
func (fxfsqr *FakeXFSQuotaRunner) getProjectIdByName(name string) int64 {
	infos := DecodeProjIdInfo([]byte(fxfsqr.projidFile))
	for _, info := range infos {
		if string(info.ProjectName) == name {
			return int64(info.ProjectId)
		}
	}
	return 0
}
func (fxfsqr *FakeXFSQuotaRunner) getProjectMountPath(projectname string) string {
	projectid := fxfsqr.getProjectIdByName(projectname)
	infos := DecodeProjectInfo([]byte(fxfsqr.projectFile))
	for _, info := range infos {
		if int64(info.ProjectId) == projectid {
			return string(info.Path)
		}
	}
	return ""
}
func (fxfsqr *FakeXFSQuotaRunner) LimitQuotaProject(project string, softLimit, hardLimit int64) ([]byte, error, int64, int64) {
	for i, _ := range fxfsqr.projects {
		if fxfsqr.projects[i].ProjectName == project {
			fxfsqr.projects[i].SoftQuota = softLimit
			fxfsqr.projects[i].HardQuota = hardLimit
			return nil, nil, softLimit, hardLimit
		}
	}
	fxfsqr.projects = append(fxfsqr.projects, FakeXFSQuotaProject{
		MountPath:   fxfsqr.getProjectMountPath(project),
		SoftQuota:   softLimit,
		HardQuota:   hardLimit,
		UsedSize:    0,
		ProjectName: project,
		ProjectId:   fxfsqr.getProjectIdByName(project),
	})
	return nil, nil, softLimit, hardLimit
}
func (fxfsqr *FakeXFSQuotaRunner) MkQuotaDir(dir string, innerDirName string) ([]byte, error) {
	fxfsqr.hostpaths = append(fxfsqr.hostpaths, dir)
	return nil, nil
}

func (fxfsqr *FakeXFSQuotaRunner) IsHostQuotaPathExist(p string) bool {
	p = path.Clean(p)
	for _, hp := range fxfsqr.hostpaths {
		if p == path.Clean(hp) {
			return true
		}
	}
	return false
}
func (fxfsqr *FakeXFSQuotaRunner) WriteFile(p, content string) error {
	fxfsqr.files[path.Clean(p)] = content
	return nil
}
func (fxfsqr *FakeXFSQuotaRunner) ReadFile(p string) string {
	content, exists := fxfsqr.files[path.Clean(p)]
	if exists == true {
		return content
	}
	return ""
}
func (fxfsqr *FakeXFSQuotaRunner) GetSubDirs(parentDir string) []string {
	parentDir = path.Clean(parentDir)
	subDirs := make([]string, 0)
	for _, existPath := range fxfsqr.hostpaths {
		if path.Dir(path.Clean(existPath)) == parentDir {
			subDirs = append(subDirs, path.Clean(existPath))
		}
	}
	return subDirs
}
