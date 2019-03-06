package prjquota

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type HostInterface interface {
	MkDir(dir string) error
	IsPathExist(path string) bool
	WriteFile(path, content string) error
	ReadFile(path string) string
	DeleteFile(path string) error
	GetSubDirs(parentDir string) []string
}

type hostRunner struct {
}

func NewHostRunner() HostInterface {
	return &hostRunner{}
}

func (hr *hostRunner) MkDir(dir string) error {
	if hr.IsPathExist(dir) == true {
		return nil
	}
	if mkdirErr := os.MkdirAll(dir, 0755); mkdirErr != nil {
		return fmt.Errorf("mkdir %s err:%v", dir, mkdirErr)
	}
	return nil
}

func (hr *hostRunner) IsPathExist(dir string) bool {
	var exist = true
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func (hr *hostRunner) DeleteFile(path string) error {
	if hr.IsPathExist(path) == true {
		return os.RemoveAll(path)
	}
	return nil
}

func (hr *hostRunner) WriteFile(filepath, content string) error {
	baseDir := path.Dir(filepath)
	if hr.IsPathExist(baseDir) == false {
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

func (hr *hostRunner) ReadFile(path string) string {
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

func (hr *hostRunner) GetSubDirs(parentDir string) []string {
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

type fakeHostRunner struct {
	hostpaths []string
	files     map[string]string
}

func NewFakeHostRunner(hostpaths []string) *fakeHostRunner {
	return &fakeHostRunner{
		hostpaths: hostpaths,
		files:     make(map[string]string),
	}
}

func (fhr *fakeHostRunner) GetDirs(prefix string) []string {
	ret := make([]string, 0, len(fhr.hostpaths))
	for _, dir := range fhr.hostpaths {
		if strings.HasPrefix(dir, prefix) {
			ret = append(ret, dir)
		}
	}
	return ret
}

func (fhr *fakeHostRunner) GetFiles(prefix string, filter string) []string {
	ret := make([]string, 0, len(fhr.files))
	for file := range fhr.files {
		if strings.HasPrefix(file, prefix) && strings.Contains(file, filter) == false {
			ret = append(ret, file)
		}
	}
	return ret
}

func (fhr *fakeHostRunner) MkDir(dir string) error {
	fhr.hostpaths = append(fhr.hostpaths, dir)
	return nil
}

func (hr *fakeHostRunner) IsPathExist(filepath string) bool {
	filepath = path.Clean(filepath)
	for _, hp := range hr.hostpaths {
		if filepath == path.Clean(hp) {
			return true
		}
	}
	for file := range hr.files {
		if file == filepath {
			return true
		}
	}
	return false
}

func (hr *fakeHostRunner) WriteFile(filepath, content string) error {
	hr.files[path.Clean(filepath)] = content
	return nil
}

func (hr *fakeHostRunner) ReadFile(filepath string) string {
	content, exists := hr.files[path.Clean(filepath)]
	if exists == true {
		return content
	}
	return ""
}

func (hr *fakeHostRunner) DeleteFile(filePath string) error {
	newFiles := make(map[string]string)
	for file, content := range hr.files {
		if strings.HasPrefix(file, filePath) == false {
			newFiles[file] = content
		}
	}
	hr.files = newFiles
	newDirs := make([]string, 0, len(hr.hostpaths))
	for _, dir := range hr.hostpaths {
		if strings.HasPrefix(dir, filePath) == false {
			newDirs = append(newDirs, dir)
		}
	}
	hr.hostpaths = newDirs
	return nil
}

func (hr *fakeHostRunner) GetSubDirs(parentDir string) []string {
	parentDir = path.Clean(parentDir)
	subDirs := make([]string, 0)
	for _, existPath := range hr.hostpaths {
		if path.Dir(path.Clean(existPath)) == parentDir {
			subDirs = append(subDirs, path.Clean(existPath))
		}
	}
	return subDirs
}
