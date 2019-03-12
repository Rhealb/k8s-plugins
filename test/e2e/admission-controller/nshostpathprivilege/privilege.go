package nshostpathprivilege

import (
	"strings"

	"github.com/k8s-plugins/test/e2e/framework"
	imageutils "github.com/k8s-plugins/test/e2e/utils/images"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = SIGDescribe("PodPrivilege", func() {
	f := framework.NewDefaultFramework("PodPrivilege")

	It("should ensure the pod has privilege can be created when it's namespace allow privilege.",
		func() {
			ensurePrivilegePodCreate(f, "ns-allow-privilege", map[string]string{"io.enndata.namespace/alpha-allowprivilege": "true"}, "")
		}, 3)
	It("should ensure the pod has privilege cann't be created when it's namespace not allow privilege.",
		func() {
			ensurePrivilegePodCreate(f, "ns-notallow-privilege", map[string]string{"io.enndata.namespace/alpha-allowprivilege": "false"}, "not support privilege")
		}, 3)
	It("should ensure the pod has privilege cann't be created when it's namespace not allow privilege.",
		func() {
			ensurePrivilegePodCreate(f, "ns-default-notallow-privilege", map[string]string{}, "not support privilege")
		}, 3)
})

func ensurePrivilegePodCreate(f *framework.Framework, namespace string, annotations map[string]string, errStr string) {
	By("Creating a test namespace")
	ns, err := f.CreateNamespace(namespace, nil, annotations)
	Expect(err).NotTo(HaveOccurred())
	By("Create pod has privilege")
	privilege := true
	var terminationGracePeriodSeconds int64 = 0
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
			Containers: []v1.Container{
				{
					Name:    "test",
					Image:   imageutils.GetAlpineImageName(),
					Command: []string{"tail", "-f", "/etc/hosts"},
					SecurityContext: &v1.SecurityContext{
						Privileged: &privilege,
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
