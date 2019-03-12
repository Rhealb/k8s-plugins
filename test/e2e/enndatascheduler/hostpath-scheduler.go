package enndatascheduler

import (
	"github.com/k8s-plugins/test/e2e/framework"
	imageutils "github.com/k8s-plugins/test/e2e/utils/images"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1node "k8s.io/kubernetes/pkg/api/v1/node"
)

var _ = SIGDescribe("EnndataScheduler", func() {
	f := framework.NewDefaultFramework("EnndataScheduler")

	nodesFilter := func(nodeList []*v1.Node, filter func(node *v1.Node) bool) []*v1.Node {
		ret := make([]*v1.Node, 0, len(nodeList))
		for _, node := range nodeList {
			if filter(node) {
				ret = append(ret, node)
			}
		}
		return ret
	}

	nodeCanSchedule := func(node *v1.Node) bool {
		if node != nil {
			if node.Spec.Unschedulable == true {
				return false
			}
			_, cond := v1node.GetNodeCondition(&node.Status, v1.NodeReady)
			if cond.Status != v1.ConditionTrue {
				return false
			}
			return true
		}
		return false
	}

	isHostpathNode := func(node *v1.Node) bool {
		if node != nil {
			if node.Labels != nil && node.Labels["node-role.kubernetes.io/hostpath"] == "true" {
				return true
			}
		}
		return false
	}

	isNodeInList := func(nodeName string, nodeList []*v1.Node) bool {
		for _, node := range nodeList {
			if node.Name == nodeName {
				return true
			}
		}
		return false
	}

	It("ensure pod use hostpath pv can be properly scheduled",
		func() {
			By("List all nodes")
			list, errList := f.ClientSet.CoreV1().Nodes().List(metav1.ListOptions{})
			Expect(errList).NotTo(HaveOccurred())
			Expect(len(list.Items) > 0).To(Equal(true))
			nodeList := make([]*v1.Node, 0, len(list.Items))
			for i := range list.Items {
				nodeList = append(nodeList, &list.Items[i])
			}

			By("filter hostpath pv nodes")
			canScheduleNodeList := nodesFilter(nodeList, nodeCanSchedule)
			hostpathNodeCanSchedulerList := nodesFilter(canScheduleNodeList, isHostpathNode)
			//			otherNodeCanSchedulerList := nodesFilter(canScheduleNodeList, func(node *v1.Node) bool { return !isHostpathNode(node) })

			By("Creating a enndataschedule-test namespace")
			ns, err := f.CreateNamespace("enndataschedule-test", nil, nil)
			Expect(err).NotTo(HaveOccurred())

			By("Creating enndataschedule Hostpath PV")
			pv, errPV := f.CreateHostPathPV("enndataschedule-hostpathpv", false, 1, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "true"})
			Expect(errPV).NotTo(HaveOccurred())

			By("Create enndataschedule Hostpath PVC")
			pvc, errPVC := f.CreateHostPathPVC("enndataschedule-hostpathpvc", ns.Name, pv.Name, 1)
			Expect(errPVC).NotTo(HaveOccurred())

			By("Creating enndataschedule CSIHostpath PV")
			pvCSI, errPVCSI := f.CreateHostPathPV("enndataschedule-csihostpathpv", true, 1, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "true"})
			Expect(errPVCSI).NotTo(HaveOccurred())

			By("Create enndataschedule CSIHostpath PVC")
			pvcCSI, errPVCCSI := f.CreateHostPathPVC("enndataschedule-csihostpathpvc", ns.Name, pvCSI.Name, 1)
			Expect(errPVCCSI).NotTo(HaveOccurred())

			By("Creating enndataschedule keep false CSIHostpath PV")
			pvCSIKeepFalse, errPVCSIKeepFalse := f.CreateHostPathPV("enndataschedule-keepfalsecsihostpathpv", true, 1, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "false"})
			Expect(errPVCSIKeepFalse).NotTo(HaveOccurred())

			By("Create enndataschedule keep false CSIHostpath PVC")
			pvcCSIKeepFalse, errPVCCSIKeepFalse := f.CreateHostPathPVC("enndataschedule-keepfalsecsihostpathpvc", ns.Name, pvCSIKeepFalse.Name, 1)
			Expect(errPVCCSIKeepFalse).NotTo(HaveOccurred())

			By("Wait enndataschedule hostpath pvc bound")
			pvcBoundErr := f.WaitForPVCStatus(pvc.Namespace, pvc.Name, v1.ClaimBound)
			Expect(pvcBoundErr).NotTo(HaveOccurred())

			By("Wait enndataschedule csi hostpath pvc bound")
			pvcCSIBoundErr := f.WaitForPVCStatus(pvcCSI.Namespace, pvcCSI.Name, v1.ClaimBound)
			Expect(pvcCSIBoundErr).NotTo(HaveOccurred())

			By("Wait enndataschedule keep false csi hostpath pvc bound")
			pvcCSIKeepFalseBoundErr := f.WaitForPVCStatus(pvcCSIKeepFalse.Namespace, pvcCSIKeepFalse.Name, v1.ClaimBound)
			Expect(pvcCSIKeepFalseBoundErr).NotTo(HaveOccurred())

			By("check hostpathpv pod1 schedule")
			hostpathpvScheduleNode1, errHostpathpvScheduleNode1 := getPodScheduleNode(f, ns.Name, "enndataschedule-hostpathpv1", nil, []*v1.PersistentVolumeClaim{pvc})
			Expect(errHostpathpvScheduleNode1).NotTo(HaveOccurred())
			Expect(isNodeInList(hostpathpvScheduleNode1, hostpathNodeCanSchedulerList)).To(Equal(true))

			By("check hostpathpv pod2 schedule")
			hostpathpvScheduleNode2, errHostpathpvScheduleNode2 := getPodScheduleNode(f, ns.Name, "enndataschedule-hostpathpv2", nil, []*v1.PersistentVolumeClaim{pvc})
			Expect(errHostpathpvScheduleNode2).NotTo(HaveOccurred())
			Expect(isNodeInList(hostpathpvScheduleNode2, hostpathNodeCanSchedulerList)).To(Equal(true))

			By("check csi hostpathpv pod1 schedule")
			csiHostpathpvScheduleNode1, errCSIHostpathpvScheduleNode1 := getPodScheduleNode(f, ns.Name, "enndataschedule-csihostpathpv1", nil, []*v1.PersistentVolumeClaim{pvcCSI})
			Expect(errCSIHostpathpvScheduleNode1).NotTo(HaveOccurred())
			Expect(isNodeInList(csiHostpathpvScheduleNode1, hostpathNodeCanSchedulerList)).To(Equal(true))

			By("check csi hostpathpv pod2 schedule")
			csiHostpathpvScheduleNode2, errCSIHostpathpvScheduleNode2 := getPodScheduleNode(f, ns.Name, "enndataschedule-csihostpathpv2", nil, []*v1.PersistentVolumeClaim{pvcCSI})
			Expect(errCSIHostpathpvScheduleNode2).NotTo(HaveOccurred())
			Expect(isNodeInList(csiHostpathpvScheduleNode2, hostpathNodeCanSchedulerList)).To(Equal(true))

			By("check keep false csi hostpathpv pod1 schedule")
			csiKeepFalseHostpathpvScheduleNode1, errCSIKeepFalseHostpathpvScheduleNode1 := getPodScheduleNode(f, ns.Name, "enndataschedule-keepfalsecsihostpathpv1", nil, []*v1.PersistentVolumeClaim{pvcCSIKeepFalse})
			Expect(errCSIKeepFalseHostpathpvScheduleNode1).NotTo(HaveOccurred())
			Expect(isNodeInList(csiKeepFalseHostpathpvScheduleNode1, hostpathNodeCanSchedulerList)).To(Equal(true))

			By("check keep false csi hostpathpv pod2 schedule")
			csiKeepFalseHostpathpvScheduleNode2, errCSIKeepFalseHostpathpvScheduleNode2 := getPodScheduleNode(f, ns.Name, "enndataschedule-keepfalsecsihostpathpv2", nil, []*v1.PersistentVolumeClaim{pvcCSIKeepFalse})
			Expect(errCSIKeepFalseHostpathpvScheduleNode2).NotTo(HaveOccurred())
			Expect(csiKeepFalseHostpathpvScheduleNode2).To(Equal(csiKeepFalseHostpathpvScheduleNode1))

		}, 30)
})

func getPodScheduleNode(f *framework.Framework, namespace, name string, nodeSelector map[string]string, pvcs []*v1.PersistentVolumeClaim) (string, error) {
	By("Create schedule pod " + name)
	if nodeSelector == nil {
		nodeSelector = map[string]string{}
	}
	var terminationGracePeriodSeconds int64 = 0
	volumes := []v1.Volume{}
	if pvcs != nil && len(pvcs) > 0 {
		for _, pvc := range pvcs {
			volumes = append(volumes, v1.Volume{
				Name: pvc.Name,
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.Name,
					},
				},
			})
		}
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			NodeSelector:                  nodeSelector,
			TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
			Volumes: volumes,
			Containers: []v1.Container{
				{
					Name:    "test",
					Image:   imageutils.GetAlpineImageName(),
					Command: []string{"tail", "-f", "/etc/hosts"},
				},
			},
		},
	}
	podCreate, err := f.ClientSet.CoreV1().Pods(namespace).Create(pod)
	Expect(err).NotTo(HaveOccurred())

	return f.WaitForPodScheduled(podCreate.Namespace, podCreate.Name)
}
