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
	"time"

	"github.com/golang/glog"

	"k8s-plugins/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	"k8s.io/client-go/kubernetes"
)

// PluginFolder defines the location of hostpathplugin
const (
	PluginFolder = "/var/lib/kubelet/plugins/hostpathplugin"
)

type hostpath struct {
	driver *csicommon.CSIDriver

	ids *identityServer
	ns  *nodeServer
	cs  *controllerServer

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

var (
	hostpathDriver *hostpath
	version        = "0.2.0"
)

func GetHostPathDriver() *hostpath {
	return &hostpath{}
}

func NewIdentityServer(d *csicommon.CSIDriver) *identityServer {
	return &identityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(d),
	}
}

func NewControllerServer(d *csicommon.CSIDriver) *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
	}
}

func NewNodeServer(d *csicommon.CSIDriver, k8sClient k8sInterface, nodeName string) *nodeServer {
	return &nodeServer{
		DefaultNodeServer:         csicommon.NewDefaultNodeServer(d),
		k8sClient:                 k8sClient,
		xfsquotamanager:           xfsquotamanager.NewXFSQuotaManager("/xfs", k8sClient),
		quotaPathUsedByMountPaths: make(map[string]map[string]bool),
		nodeName:                  nodeName,
	}
}

func (hostpath *hostpath) Run(driverName, nodeID, nodeName, endpoint string, client kubernetes.Interface, informerResync time.Duration) {
	glog.Infof("Driver: %v", driverName)

	// Initialize default library driver
	hostpath.driver = csicommon.NewCSIDriver(driverName, version, nodeID)
	if hostpath.driver == nil {
		glog.Fatalln("Failed to initialize CSI Driver.")
	}
	hostpath.driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	})
	hostpath.driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER})

	// Create GRPC servers
	hostpath.ids = NewIdentityServer(hostpath.driver)
	k8sController := &k8scontroller{client: client}

	if ok, curVer, minVer := k8sController.IsNodeSupport(nodeName); ok == false {
		glog.Errorf("node %s verion:%s is not support this plugin, should higher than %s", nodeName, curVer, minVer)
		return
	}
	hostpath.ns = NewNodeServer(hostpath.driver, k8sController, nodeName)
	hostpath.cs = NewControllerServer(hostpath.driver)
	s := csicommon.NewNonBlockingGRPCServer()
	if err := k8sController.Start(informerResync); err != nil {
		glog.Errorf("start k8sController err:%v", err)
		return
	}

	syncWork := &SyncWorker{
		k8sClient:              k8sController,
		xfsquotamanager:        hostpath.ns.xfsquotamanager,
		driverName:             driverName,
		nodeName:               nodeName,
		isQuotPathUseInterface: hostpath.ns,
		shouldDeleteQuotaPaths: make(map[string]bool),
	}
	syncWork.Start(10 * time.Second)
	s.Start(endpoint, hostpath.ids, hostpath.cs, hostpath.ns)
	s.Wait()
}
