package common

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

const (
	XfsKeepForOnePodSubIdFile string = "curowner"
	XfsQuotaProjectPrifix     string = "k8spro"
	XfsQuotaProjIdStart       int64  = 1000000
	XfsQuotaMaxProjIdNum      int64  = 2000
	XfsQuotaDirPrifix         string = "k8squota"
	XfsDefaultSaveSize        int64  = 0
	//xfsHistroyNextIdName      string = "nextid"
	XfsKeepForOnePodFlag     string = "kfp"
	XfsKeepForOnePodInnerDir string = "volume"
	XfsCSISaveDirName        string = "csis"
	XfsCSIFlagFile           string = "csi"

	XfsMountedToPathsFile  string = "mounts"
	XfsOwnerIDSaveDirName  string = "ownerids"
	XFSStatusFileName      string = "status"
	XfsDiskDisablefilename string = "disable"

	// record the node which have pod mount the pv
	PVVolumeHostPathMountNode = "io.enndata.kubelet/alpha-pvchostpathnode"

	PVHostPathMountPolicyAnn = "io.enndata.user/alpha-pvhostpathmountpolicy"

	// only for io.enndata.user/alpha-pvhostpathmountpolicy=keep
	PVHostPathMountTimeoutAnn   = "io.enndata.user/alpha-pvhostpathmounttimeout"
	PVHostPathTimeoutDelPodAnn  = "io.enndata.user/alpha-pvhostpathtimeoutdeletepod"
	NodeDiskQuotaInfoAnn        = "io.enndata.kubelet/alpha-nodediskquotainfo"
	NodeDiskQuotaStatusAnn      = "io.enndata.kubelet/alpha-nodediskquotastate"
	NodeDiskQuotaDisableListAnn = "io.enndata.kubelet/alpha-nodediskquotadisablelist"

	PVHostPathCapacityAnn      = "io.enndata.user/alpha-pvhostpathcapcity"
	PVHostPathCapacityStateAnn = "io.enndata.kubelet/alpha-pvhostpathcapcitystate"
	PVHostPathScaleStateAnn    = "io.enndata.kubelet/alpha-pvhostpathscalestate"
	PVHostPathQuotaForOnePod   = "io.enndata.user/alpha-pvhostpathquotaforonepod"
	PVHostPathKeep             = "keep"
	PVHostPathNone             = "none"
)

func GetKeepIdFromDirName(basename string) (keepid, podid string) {
	strs := strings.Split(basename, "_")
	if len(strs) != 3 || strs[0] != XfsQuotaDirPrifix || strings.HasPrefix(strs[2], XfsKeepForOnePodFlag) == false {
		return "", ""
	} else {
		return strs[2], strs[2][len(XfsKeepForOnePodFlag)+1:]
	}
}

func GetBaseNameFromPath(mountPath string) string {
	reg := regexp.MustCompile(XfsQuotaDirPrifix + "[-_a-zA-Z0-9]+")
	strs := reg.FindAllString(mountPath, 1)
	if len(strs) == 1 {
		return strs[0]
	}
	return ""
}

func GetInfoFromDirName(filepath string, readFun func(path string) string) (volumeId, podId, ownerId string, isKeep, isShare bool, err error) {
	name := GetBaseNameFromPath(filepath)
	strs := strings.Split(name, "_")
	if (len(strs) != 2 && len(strs) != 3) || strs[0] != XfsQuotaDirPrifix {
		return "", "", "", false, false, fmt.Errorf("[%s] is not valid k8s xfs quota dir", name)
	}
	if len(strs) == 2 {
		return strs[1], "", "", true, true, nil
	} else if len(strs) == 3 && strings.HasPrefix(strs[2], XfsKeepForOnePodFlag) {
		return strs[1], strs[2][len(XfsKeepForOnePodFlag)+1:], GetOwnerId(filepath, readFun), true, false, nil
	}
	return strs[1], strs[2], GetOwnerId(filepath, readFun), false, false, nil
}

func GetOwnerId(dir string, readFun func(path string) string) string {
	var ret string
	ret = readFun(GetOwinderIdFilePath(dir))
	if ret != "" {
		return ret
	}
	// old version and CIS the ownerid file is save as (path + "/" + xfsKeepForOnePodSubIdFile)
	ret = readFun(path.Join(dir, XfsKeepForOnePodSubIdFile))
	return ret
}

func GetOwinderIdFilePath(dir string) string {
	dir = path.Clean(dir)
	name := path.Base(dir)
	dir = path.Dir(dir)
	return path.Join(dir, XfsOwnerIDSaveDirName, name)
}

func SetOwnerId(dir, ownerId string, writeFun func(path, data string) error) error {
	// TODO: this code should be removed after xfsquotamanager in kubelet was removed
	fp := GetOwinderIdFilePath(dir)
	writeFun(fp, ownerId)

	return writeFun(path.Join(dir, XfsKeepForOnePodSubIdFile), ownerId)
}

func GetDirName(volumeid, podId string, recycle bool) string {
	if podId == "" {
		return fmt.Sprintf("%s_%s", XfsQuotaDirPrifix, volumeid)
	} else {
		if recycle == true {
			return fmt.Sprintf("%s_%s_%s-%s", XfsQuotaDirPrifix, volumeid, XfsKeepForOnePodFlag, podId)
		} else {
			return fmt.Sprintf("%s_%s_%s", XfsQuotaDirPrifix, volumeid, podId)
		}
	}
}
