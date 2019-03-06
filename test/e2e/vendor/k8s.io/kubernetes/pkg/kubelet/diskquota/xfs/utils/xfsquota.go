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
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sync"
	"time"

	"github.com/golang/glog"
	utilexec "k8s.io/utils/exec"
)

const (
	cmdXFSQuota         string = "xfs_quota"
	cmdMkQuotaDir       string = "mkdir"
	cmdWhichXFSQuota    string = "which"
	xfsProjectsPath     string = "/etc/projects"
	xfsProjidPath       string = "/etc/projid"
	xfsHistoryQuotaPath string = "/etc/historyprojects"
	xfsMinQuota         int64  = 1 // 1MB
)

type Interface interface {
	CurSystemSupport() (bool, error)
	QuotaState() ([]byte, error)
	QuotaPrint(paths []string) ([]byte, error)
	QuotaUsedInfo(paths []string) ([]byte, error)
	QuotaProjects() ([]byte, error)
	QuotaProjid() ([]byte, error)
	HistoryQuota() ([]byte, error)
	QuotaReport(paths []string) ([]byte, error)
	SaveQuotaProjectsAndId(projects, projectid string) error
	SaveHistoryQuotaInfo(info string) error
	SetupQuotaProject(project string, paths []string) ([]byte, error)
	LimitQuotaProject(project string, softLimit, hardLimit int64) ([]byte, error, int64, int64)
	MkQuotaDir(dir string, innerDirName string) ([]byte, error)
	IsHostQuotaPathExist(path string) bool
	WriteFile(path, content string) error
	ReadFile(path string) string
	GetSubDirs(parentDir string) []string
}

type runner struct {
	exec utilexec.Interface
	mu   sync.Mutex
}

func New(exec utilexec.Interface) Interface {
	return &runner{
		exec: exec,
	}
}

func (runner *runner) QuotaState() ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	// run and return
	glog.V(4).Infof("running: %s -x -c 'state'", cmdXFSQuota)
	return runner.exec.Command(cmdXFSQuota, []string{"-x", "-c", "state"}...).CombinedOutput()
}

func (runner *runner) QuotaPrint(paths []string) ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	// run and return
	glog.V(4).Infof("running: %s -x -c 'print' %v", cmdXFSQuota, paths)
	args := []string{"-x", "-c", "print"}
	if paths != nil {
		args = append(args, paths...)
	}
	return runner.exec.Command(cmdXFSQuota, args...).CombinedOutput()
}

func (runner *runner) QuotaUsedInfo(paths []string) ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	// run and return
	glog.V(4).Infof("running: %s -x -c 'df ' %v", cmdXFSQuota, paths)
	args := []string{"-x", "-c", "df "}
	if paths != nil {
		args = append(args, paths...)
	}
	return runner.exec.Command(cmdXFSQuota, args...).CombinedOutput()
}

func (runner *runner) QuotaProjects() ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	// run and return
	glog.V(4).Infof("running: cat %s", xfsProjectsPath)
	return runner.exec.Command("cat", []string{xfsProjectsPath}...).CombinedOutput()
}
func (runner *runner) QuotaProjid() ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	// run and return
	glog.V(4).Infof("running: cat %s", xfsProjidPath)
	return runner.exec.Command("cat", []string{xfsProjidPath}...).CombinedOutput()
}
func (runner *runner) HistoryQuota() ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	// run and return
	glog.V(4).Infof("running: cat %s", xfsHistoryQuotaPath)
	return runner.exec.Command("cat", []string{xfsHistoryQuotaPath}...).CombinedOutput()
}

func (runner *runner) QuotaReport(paths []string) ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	// run and return
	glog.V(4).Infof("running: %s -x -c 'report -pb'", cmdXFSQuota)
	args := []string{"-x", "-c", "report -pb"}
	if paths != nil {
		args = append(args, paths...)
	}
	return runner.exec.Command(cmdXFSQuota, args...).CombinedOutput()
}

func (runner *runner) SaveHistoryQuotaInfo(info string) error {
	fileHistory, err := os.OpenFile(xfsHistoryQuotaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil || fileHistory == nil {
		return fmt.Errorf("file %s open error %v\n", xfsHistoryQuotaPath, err)
	}
	defer fileHistory.Close()
	_, e := io.WriteString(fileHistory, info)
	if e != nil {
		return fmt.Errorf("file %s write error %v\n", xfsHistoryQuotaPath, e)
	}
	return nil
}

func (runner *runner) SaveQuotaProjectsAndId(projects, projectid string) error {

	fileProjects, err1 := os.OpenFile(xfsProjectsPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err1 != nil || fileProjects == nil {
		return fmt.Errorf("file %s open error %v\n", xfsProjectsPath, err1)
	}
	defer fileProjects.Close()
	_, e := io.WriteString(fileProjects, projects)
	if e != nil {
		return fmt.Errorf("file %s write error %v\n", xfsProjectsPath, e)
	}

	fileProjectid, err2 := os.OpenFile(xfsProjidPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err2 != nil || fileProjectid == nil {
		return fmt.Errorf("file %s open error %v\n", xfsProjidPath, err2)
	}
	defer fileProjectid.Close()
	_, e = io.WriteString(fileProjectid, projectid)
	if e != nil {
		return fmt.Errorf("file %s write error %v\n", fileProjectid, e)
	}
	return nil

}

func (runner *runner) SetupQuotaProject(project string, paths []string) ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	//glog.V(4).Infof("running: xfs_quota -x -c 'project -s %s'", project)
	//return runner.exec.Command(cmdXFSQuota, []string{"-x", "-c", fmt.Sprintf("project -s %s", project)}...).CombinedOutput()

	args := []string{"-x", "-c", fmt.Sprintf("project -s %s", project)}
	if paths != nil {
		args = append(args, paths...)
	}
	cmd := runner.exec.Command(cmdXFSQuota, args...)
	stop := make(chan struct{}, 0)
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10000 * time.Millisecond):
				cmd.Stop()
				glog.Errorf("'xfs_quota -x -c project -s %s' 10s timeout", project)
			}
		}
	}()
	return cmd.CombinedOutput()
}

func (runner *runner) LimitQuotaProject(project string, softLimit, hardLimit int64) ([]byte, error, int64, int64) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	softLimit = softLimit / 1024 / 1024
	if softLimit < xfsMinQuota {
		softLimit = xfsMinQuota
	}
	hardLimit = hardLimit / 1024 / 1024
	if hardLimit < xfsMinQuota {
		hardLimit = xfsMinQuota
	}

	glog.V(4).Infof("running: xfs_quota -x -c 'limit -p bsoft=%dM bhard=%dM %s'", softLimit, hardLimit, project)
	buf, _ := runner.exec.Command(cmdXFSQuota, []string{"-x", "-c", fmt.Sprintf("limit -p bsoft=%dM bhard=%dM %s",
		softLimit, hardLimit, project)}...).CombinedOutput()
	return buf, nil, softLimit * 1024 * 1024, hardLimit * 1024 * 1024
}
func (runner *runner) MkQuotaDir(dir string, innerDirName string) ([]byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()

	glog.V(4).Infof("running: %s -p %s'", cmdMkQuotaDir, dir)
	if mkdirErr := os.MkdirAll(dir, 0777); mkdirErr != nil {
		return []byte{}, mkdirErr
	}

	os.Chmod(dir, 0777)
	if innerDirName == "" {
		return []byte{}, nil
	}
	volumeDir := dir + "/" + innerDirName
	if mkdirErr := os.MkdirAll(volumeDir, 0777); mkdirErr != nil {
		return []byte{}, mkdirErr
	}
	return []byte{}, os.Chmod(volumeDir, 0777)
}
func (runner *runner) CurSystemSupport() (bool, error) {
	bytes, err := runner.exec.Command(cmdWhichXFSQuota, cmdXFSQuota).CombinedOutput()
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
func (runner *runner) IsHostQuotaPathExist(path string) bool {
	var exist = true
	if _, err := os.Stat(path); os.IsNotExist(err) {
		exist = false
	}
	return exist
}
func (runner *runner) WriteFile(filepath, content string) error {
	baseDir := path.Dir(filepath)
	if runner.IsHostQuotaPathExist(baseDir) == false {
		if errMake := os.MkdirAll(baseDir, 774); errMake != nil {
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
	file.Sync()
	return nil
}
func (runner *runner) ReadFile(path string) string {
	file, err := os.Open(path)
	if err != nil || file == nil {
		return ""
	}
	defer file.Close()
	buf, e := ioutil.ReadAll(file)
	if e != nil {
		return ""
	}
	return string(buf)
}

func (runner *runner) GetSubDirs(parentDir string) []string {
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
