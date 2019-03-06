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
	"strconv"
	"strings"
)

type XFSProjectName string
type XFSProjectPath string
type XFSProjectID int64
type XFSDisk int64

type XFSProjectInfo struct {
	ProjectId XFSProjectID
	Path      XFSProjectPath
}

type XFSProjectInfoList []XFSProjectInfo

func (xpil *XFSProjectInfoList) Add(proid XFSProjectID, path XFSProjectPath) {
	for _, item := range *xpil {
		if item.ProjectId == proid {
			return
		}
	}
	*xpil = append(*xpil, XFSProjectInfo{ProjectId: proid, Path: path})
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

type XFSProjIdInfo struct {
	ProjectName XFSProjectName
	ProjectId   XFSProjectID
}
type XFSProjIdInfoList []XFSProjIdInfo

func (xpiil *XFSProjIdInfoList) Add(proid XFSProjectID, proName XFSProjectName) {
	for _, item := range *xpiil {
		if item.ProjectId == proid {
			return
		}
	}
	*xpiil = append(*xpiil, XFSProjIdInfo{ProjectId: proid, ProjectName: proName})
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

type XFSProjectReportInfo struct {
	ProjectName XFSProjectName
	Used        XFSDisk
	Soft        XFSDisk
	Hard        XFSDisk
}

type XFSUsedInfo struct {
	Device    XFSDevice
	MountPath XFSMountPath
	Size      int64
	Used      int64
}
type XFSProjectReportSet map[XFSProjectName]XFSProjectReportInfo

type XFSMountPath string
type XFSDevice string

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

func DecodeReportInfo(info []byte) XFSProjectReportSet {
	ret := make(XFSProjectReportSet)
	readIndex := 0
	// find beginning of table
	started := false
	for readIndex < len(info) {
		line, n := readLine(readIndex, info)

		readIndex = n
		if len(line) == 0 {
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
		strs := MySplit(line)
		if len(strs) == 6 {
			used, err1 := strconv.ParseInt(strs[1], 10, 64)
			soft, err2 := strconv.ParseInt(strs[2], 10, 64)
			hard, err3 := strconv.ParseInt(strs[3], 10, 64)
			if err1 == nil && err2 == nil && err3 == nil {
				ret[XFSProjectName(strs[0])] = XFSProjectReportInfo{
					ProjectName: XFSProjectName(strs[0]),
					Used:        XFSDisk(used),
					Soft:        XFSDisk(soft),
					Hard:        XFSDisk(hard),
				}
			}
		}
	}
	return ret
}
func DecodeStateInfo(info []byte) []XFSStateInfo {
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
	return ret
}

func DecodeUsedInfo(info []byte) []XFSUsedInfo {
	ret := make([]XFSUsedInfo, 0)
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
			strs := MySplit(line)
			if len(strs) == 6 {
				size, err1 := strconv.ParseInt(strs[1], 10, 64)
				used, err2 := strconv.ParseInt(strs[2], 10, 64)
				if err1 == nil && err2 == nil {
					if _, exists := mountedPath[strs[5]]; exists == false {
						ret = append(ret, XFSUsedInfo{
							Device:    XFSDevice(strs[0]),
							MountPath: XFSMountPath(strs[5]),
							Size:      size,
							Used:      used,
						})
						mountedPath[strs[5]] = true
					}
				}
			}
		}
	}
	return ret
}

func DecodeProjectInfo(info []byte) XFSProjectInfoList {
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

func DecodeProjIdInfo(info []byte) []XFSProjIdInfo {
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
func isReportStart(line string) bool {
	line = strings.Replace(line, " ", "", -1)
	if strings.Repeat("-", len(line)) == line {
		return true
	}
	return false
}
func MySplit(line string) []string {
	strs := strings.Split(line, " ")
	ret := make([]string, 0, len(strs))
	for _, str := range strs {
		if str != "" {
			ret = append(ret, str)
		}
	}
	return ret
}
