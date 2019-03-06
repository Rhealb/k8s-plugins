# K8splugins 

## Explanation

[English](README.md) | [中文](README-zh.md)

K8splugins is an extension of the k8s, which can be customized privately without modifying the k8s code. All these features run on the k8s cluster in the form of plugins or Pod, which makes it easier to upgrade both k8s and plugins.

## 代码

K8splugins repo only includes some E2E test code. The specific plugin code is introduced in the form of submodule.

* https://github.com/Rhealb/cloud-controller
* https://github.com/Rhealb/kubectl-plugins
* https://github.com/Rhealb/extender-scheduler
* https://github.com/Rhealb/extender-controller-manager
* https://github.com/Rhealb/csi-plugin
* https://github.com/Rhealb/admission-controller
