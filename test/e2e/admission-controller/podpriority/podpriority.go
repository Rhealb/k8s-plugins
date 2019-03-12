package podpriority

import (
	"github.com/k8s-plugins/test/e2e/framework"
	imageutils "github.com/k8s-plugins/test/e2e/utils/images"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = SIGDescribe("PodPriority", func() {
	f := framework.NewDefaultFramework("PodPriority")

	It("should ensure the default pod's priority.",
		func() {
			By("Creating a podpriority namespace")
			ns, err := f.CreateNamespace("podpriority", nil, nil)
			Expect(err).NotTo(HaveOccurred())

			By("Creating Hostpath PV")
			pv, errPV := f.CreateHostPathPV("hostpathpv", false, 1, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "true"})
			Expect(errPV).NotTo(HaveOccurred())

			By("Create Hostpath PVC")
			pvc, errPVC := f.CreateHostPathPVC("hostpathpvc", ns.Name, pv.Name, 1)
			Expect(errPVC).NotTo(HaveOccurred())

			By("Creating CSIHostpath PV")
			pvCSI, errPVCSI := f.CreateHostPathPV("csihostpathpv", true, 1, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "true"})
			Expect(errPVCSI).NotTo(HaveOccurred())

			By("Create CSIHostpath PVC")
			pvcCSI, errPVCCSI := f.CreateHostPathPVC("csihostpathpvc", ns.Name, pvCSI.Name, 1)
			Expect(errPVCCSI).NotTo(HaveOccurred())

			By("Wait hostpath pvc bound")
			pvcBoundErr := f.WaitForPVCStatus(pvc.Namespace, pvc.Name, v1.ClaimBound)
			Expect(pvcBoundErr).NotTo(HaveOccurred())

			By("Wait csi hostpath pvc bound")
			pvcCSIBoundErr := f.WaitForPVCStatus(pvcCSI.Namespace, pvcCSI.Name, v1.ClaimBound)
			Expect(pvcCSIBoundErr).NotTo(HaveOccurred())

			priorityDefault := getPodCreatePriority(f, map[string]string{}, ns.Name, "prioritydefault", nil, false)
			Expect(priorityDefault).To(Equal(int32(1000)))

			priorityHostPath := getPodCreatePriority(f, map[string]string{}, ns.Name, "priorityhostpath", []*v1.PersistentVolumeClaim{pvc}, false)
			Expect(priorityHostPath).To(Equal(int32(2000)))

			priorityHostPathCSI := getPodCreatePriority(f, map[string]string{}, ns.Name, "priorityhostpathcsi", []*v1.PersistentVolumeClaim{pvcCSI}, false)
			Expect(priorityHostPathCSI).To(Equal(int32(2000)))

			priorityCritical := getPodCreatePriority(f, map[string]string{"scheduler.alpha.kubernetes.io/critical-pod": "true"}, ns.Name, "prioritycritical", nil, false)
			Expect(priorityCritical).To(Equal(int32(3000)))

			priorityCriticalHostPath := getPodCreatePriority(f, map[string]string{"scheduler.alpha.kubernetes.io/critical-pod": "true"}, ns.Name, "prioritycriticalhostpath", []*v1.PersistentVolumeClaim{pvcCSI}, false)
			Expect(priorityCriticalHostPath).To(Equal(int32(4000)))

			prioritySystem := getPodCreatePriority(f, map[string]string{}, "kube-system", "prioritysystem", nil, true)
			Expect(prioritySystem).To(Equal(int32(5000)))
		}, 10)
})

func getPodCreatePriority(f *framework.Framework, anns map[string]string, namespace, name string, pvcs []*v1.PersistentVolumeClaim, deletePod bool) int32 {
	By("Create priority pod " + name)
	if anns == nil {
		anns = map[string]string{}
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
			Name:        name,
			Annotations: anns,
		},
		Spec: v1.PodSpec{
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
	Expect(podCreate.Spec.Priority).NotTo(BeNil())
	priority := *podCreate.Spec.Priority
	if deletePod {
		errDelete := f.ClientSet.CoreV1().Pods(namespace).Delete(podCreate.Name, nil)
		Expect(errDelete).NotTo(HaveOccurred())
	}
	return priority
}
