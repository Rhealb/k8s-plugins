package nshostpathprivilege

import (
	"strings"

	imageutils "github.com/k8s-plugins/test/e2e/utils/images"

	"github.com/k8s-plugins/test/e2e/framework"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = SIGDescribe("PodHostpath", func() {
	f := framework.NewDefaultFramework("PodHostpath")

	It("should ensure the pod has hostpath can be created when it's namespace allow hostpath.",
		func() {
			ensureHostpathPodCreate(f, "ns-allow-hostpath", map[string]string{"io.enndata.namespace/alpha-allowhostpath": "true"}, "")
		}, 3)
	It("should ensure the pod has hostpath cann't be created when it's namespace not allow hostpath.",
		func() {
			ensureHostpathPodCreate(f, "ns-notallow-hostpath", map[string]string{"io.enndata.namespace/alpha-allowhostpath": "false"}, "not support hostpath")
		}, 3)
	It("should ensure the pod has hostpath cann't be created when it's namespace not allow hostpath.",
		func() {
			ensureHostpathPodCreate(f, "ns-default-notallow-hostpath", map[string]string{}, "not support hostpath")
		}, 3)
})

func ensureHostpathPodCreate(f *framework.Framework, namespace string, annotations map[string]string, errStr string) {
	By("Creating a test namespace")
	ns, err := f.CreateNamespace(namespace, nil, annotations)
	Expect(err).NotTo(HaveOccurred())
	By("Create pod has hostpath volume")
	var terminationGracePeriodSeconds int64 = 0
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
			Volumes: []v1.Volume{
				{
					Name: "hp",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/tmp/test-pod",
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:    "test",
					Image:   imageutils.GetAlpineImageName(),
					Command: []string{"tail", "-f", "/etc/hosts"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "hp",
							MountPath: "/mnt/hp",
						},
					},
				},
			},
		},
	}
	pod, err = f.ClientSet.CoreV1().Pods(ns.Name).Create(pod)
	if errStr == "" {
		Expect(err).NotTo(HaveOccurred())
	} else {
		Expect(strings.Contains(err.Error(), errStr)).To(Equal(true))
	}
}
