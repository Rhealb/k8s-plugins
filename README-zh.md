# K8splugins 

K8splugins 是对k8s集群功能的扩充，在不修改k8s代码的前提下对其做一些私有订制．所有这些功能多以插件的形式或者Pod的形式运行在k8s集群上，这使得无论是k8s升级还是插件升级变得更容易．

## 代码

K8splugins这个repo只是放了一些e2e test代码，具体的插件代码是以submodule的形式引入，具体可以参考：

* https://github.com/Rhealb/cloud-controller
* https://github.com/Rhealb/kubectl-plugins
* https://github.com/Rhealb/extender-scheduler
* https://github.com/Rhealb/extender-controller-manager
* https://github.com/Rhealb/csi-plugin
* https://github.com/Rhealb/admission-controller

