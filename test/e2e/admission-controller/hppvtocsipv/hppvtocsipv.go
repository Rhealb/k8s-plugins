package hppvtocsipv

import (
	"github.com/k8s-plugins/test/e2e/framework"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = SIGDescribe("PVToCSIPV", func() {
	f := framework.NewDefaultFramework("PVToCSIPV")

	It("should ensure hostpath pv transform to csi hostpath pv.",
		func() {
			By("Creating hostpathpv-to-csi PV")
			pv, errPV := f.CreateHostPathPV("hostpathpv-to-csi", false, 1, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "true"})
			Expect(errPV).NotTo(HaveOccurred())
			Expect(pv.Spec.CSI).NotTo(BeNil())
			Expect(pv.Spec.CSI.Driver).To(Equal("xfshostpathplugin"))
		}, 5)
})
