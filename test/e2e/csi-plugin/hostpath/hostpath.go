package hostpath

import (
	"bytes"
	"fmt"
	"io"
	"k8s-plugins/test/e2e/framework"
	imageutils "k8s-plugins/test/e2e/utils/images"
	"os"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/wait"

	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
)

var _ = SIGDescribe("CSIHostPath", func() {
	f := framework.NewDefaultFramework("CSIHostPath")

	It("should ensure none false csi hostpath pv features.",
		func() {
			var testSize int64 = 10 * 1024 * 1024
			By("Creating a csi hostpath pv test namespace")
			ns, err := f.CreateNamespace("csihostpathnonefalsetest", nil, nil)
			Expect(err).NotTo(HaveOccurred())

			By("Creating CSIHostpath test PV")
			pvCSI, errPVCSI := f.CreateHostPathPV("csihostpathnonefalsetest", true, testSize, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "none", "io.enndata.user/alpha-pvhostpathquotaforonepod": "false"})
			Expect(errPVCSI).NotTo(HaveOccurred())

			By("Create CSIHostpath test PVC")
			pvcCSI, errPVCCSI := f.CreateHostPathPVC("csihostpathnonefalsetest", ns.Name, pvCSI.Name, testSize)
			Expect(errPVCCSI).NotTo(HaveOccurred())

			By("Wait csi hostpath pvc bound")
			pvcCSIBoundErr := f.WaitForPVCStatus(pvcCSI.Namespace, pvcCSI.Name, v1.ClaimBound)
			Expect(pvcCSIBoundErr).NotTo(HaveOccurred())

			By("Create use csi hostpath pv pod")
			testPod, mountPaths := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, true)
			Expect(testPod).NotTo(BeNil())
			Expect(len(mountPaths)).To(Equal(1))

			By("Wait pod running")
			curPod, waitPhaseErr := f.WaitForPodPhase(testPod.Namespace, testPod.Name, v1.PodRunning)
			Expect(waitPhaseErr).NotTo(HaveOccurred())
			Expect(curPod).NotTo(BeNil())

			By("sleep 3 seconds")
			time.Sleep(3 * time.Second)

			By("Get pod csi mount path info")
			capcity, used, _ := getCSIPathInfo(f, testPod.Namespace, testPod.Name, testPod.Spec.Containers[0].Name, mountPaths[0])
			Expect(capcity).To(Equal(testSize / 1024)) // capcity is KB
			Expect(used <= capcity).To(Equal(true))

			By("Wait pv mount info update")
			errWait := wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 0 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info")
			mountInfo, errGetInfo := f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
			Expect(mountInfo[0].MountInfos[0].PodInfo).To(BeNil())

			By("Create use csi hostpath pv pod2")
			testPod2, mountPaths2 := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest2", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, false)
			Expect(testPod2).NotTo(BeNil())
			Expect(len(mountPaths2)).To(Equal(1))

			By("Wait pod2 running")
			curPod2, waitPhaseErr2 := f.WaitForPodPhase(testPod2.Namespace, testPod2.Name, v1.PodRunning)
			Expect(waitPhaseErr2).NotTo(HaveOccurred())
			Expect(curPod2).NotTo(BeNil())

			By("Get pod2 csi mount path info")
			capcity2, used2, _ := getCSIPathInfo(f, testPod2.Namespace, testPod2.Name, testPod2.Spec.Containers[0].Name, mountPaths2[0])
			Expect(capcity2).To(Equal(testSize / 1024)) // capcity is KB
			Expect(used2 <= capcity2).To(Equal(true))

			By("Sleep 20 seconds")
			time.Sleep(20 * time.Second)

			By("Check mount info")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			if len(mountInfo) == 1 { // the same node share the quota path
				Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
				Expect(curPod2.Status.HostIP).To(Equal(curPod.Status.HostIP))
				Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
				Expect(mountInfo[0].MountInfos[0].PodInfo).To(BeNil())
			} else {
				Expect(len(mountInfo)).To(Equal(2))
				Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
				Expect(len(mountInfo[1].MountInfos)).To(Equal(1))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
				Expect(mountInfo[0].MountInfos[0].PodInfo).To(BeNil())
				Expect(mountInfo[1].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
				Expect(mountInfo[1].MountInfos[0].PodInfo).To(BeNil())
				if mountInfo[0].NodeName == curPod.Status.HostIP {
					Expect(mountInfo[1].NodeName).To(Equal(curPod2.Status.HostIP))
				} else {
					Expect(mountInfo[0].NodeName).To(Equal(curPod2.Status.HostIP))
					Expect(mountInfo[1].NodeName).To(Equal(curPod.Status.HostIP))
				}
			}

			By("Delete 2 pods")
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				errDelete := f.DeleteAndWaitPodDelete(testPod.Namespace, testPod.Name)
				Expect(errDelete).NotTo(HaveOccurred())
			}()
			go func() {
				defer wg.Done()
				errDelete := f.DeleteAndWaitPodDelete(testPod2.Namespace, testPod2.Name)
				Expect(errDelete).NotTo(HaveOccurred())
			}()
			wg.Wait()

			By("Wait pv mount info update")
			errWait = wait.PollImmediate(1*time.Second, 40*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				sum := 0
				for _, mi := range mountInfo {
					sum += len(mi.MountInfos)
				}
				if sum == 0 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

		}, 100)
	It("should ensure keep false csi hostpath pv features.",
		func() {
			var testSize int64 = 10 * 1024 * 1024
			By("Creating a csi hostpath pv test namespace")
			ns, err := f.CreateNamespace("csihostpathkeepfalsetest", nil, nil)
			Expect(err).NotTo(HaveOccurred())

			By("Creating CSIHostpath test PV")
			pvCSI, errPVCSI := f.CreateHostPathPV("csihostpathkeepfalsetest", true, testSize, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "false"})
			Expect(errPVCSI).NotTo(HaveOccurred())

			By("Create CSIHostpath test PVC")
			pvcCSI, errPVCCSI := f.CreateHostPathPVC("csihostpathkeepfalsetest", ns.Name, pvCSI.Name, testSize)
			Expect(errPVCCSI).NotTo(HaveOccurred())

			By("Wait csi hostpath pvc bound")
			pvcCSIBoundErr := f.WaitForPVCStatus(pvcCSI.Namespace, pvcCSI.Name, v1.ClaimBound)
			Expect(pvcCSIBoundErr).NotTo(HaveOccurred())

			By("Create use csi hostpath pv pod")
			testPod, mountPaths := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, true)
			Expect(testPod).NotTo(BeNil())
			Expect(len(mountPaths)).To(Equal(1))

			By("Wait pod running")
			curPod, waitPhaseErr := f.WaitForPodPhase(testPod.Namespace, testPod.Name, v1.PodRunning)
			Expect(waitPhaseErr).NotTo(HaveOccurred())
			Expect(curPod).NotTo(BeNil())

			By("sleep 3 seconds")
			time.Sleep(3 * time.Second)

			By("Get pod csi mount path info")
			capcity, used, _ := getCSIPathInfo(f, testPod.Namespace, testPod.Name, testPod.Spec.Containers[0].Name, mountPaths[0])
			Expect(capcity).To(Equal(testSize / 1024)) // capcity is KB
			Expect(used <= capcity).To(Equal(true))

			By("Create use csi hostpath pv pod2")
			testPod2, mountPaths2 := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest2", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, false)
			Expect(testPod2).NotTo(BeNil())
			Expect(len(mountPaths2)).To(Equal(1))

			By("Wait pod2 running")
			curPod2, waitPhaseErr2 := f.WaitForPodPhase(testPod2.Namespace, testPod2.Name, v1.PodRunning)
			Expect(waitPhaseErr2).NotTo(HaveOccurred())
			Expect(curPod2).NotTo(BeNil())
			Expect(curPod2.Status.HostIP).To(Equal(curPod.Status.HostIP))

			By("Get pod2 csi mount path info")
			capcity2, used2, _ := getCSIPathInfo(f, testPod2.Namespace, testPod2.Name, testPod2.Spec.Containers[0].Name, mountPaths2[0])
			Expect(capcity2).To(Equal(testSize / 1024)) // capcity is KB
			Expect(used2 <= capcity2).To(Equal(true))
			Expect(used2 >= testSize/1024).To(Equal(true))
			Expect(used2 >= used).To(Equal(true))

			By("Wait pv mount info update")
			errWait := wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 0 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info")
			mountInfo, errGetInfo := f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
			Expect(mountInfo[0].MountInfos[0].PodInfo).To(BeNil())

			By("Scale pv's capcity")
			errScale := f.UpdateHostPathPVCapcity(pvCSI.Name, testSize*2)
			Expect(errScale).NotTo(HaveOccurred())

			By("Wait pv's mount info update")
			errWait = wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 0 && mountInfo[0].MountInfos[0].VolumeQuotaSize >= testSize*2 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info after scale")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
			Expect(mountInfo[0].MountInfos[0].PodInfo).To(BeNil())

			By("Get pod csi mount path info again")
			capcity, used, _ = getCSIPathInfo(f, testPod.Namespace, testPod.Name, testPod.Spec.Containers[0].Name, mountPaths[0])
			Expect(capcity).To(Equal(testSize * 2 / 1024)) // capcity is KB
			Expect(used <= capcity).To(Equal(true))

			By("Get pod2 csi mount path info again")
			capcity2, used2, _ = getCSIPathInfo(f, testPod2.Namespace, testPod2.Name, testPod2.Spec.Containers[0].Name, mountPaths2[0])
			Expect(capcity2).To(Equal(testSize * 2 / 1024)) // capcity is KB
			Expect(used2 <= capcity2).To(Equal(true))

			By("Delete 2 pods")
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				errDelete := f.DeleteAndWaitPodDelete(testPod.Namespace, testPod.Name)
				Expect(errDelete).NotTo(HaveOccurred())
			}()
			go func() {
				defer wg.Done()
				errDelete := f.DeleteAndWaitPodDelete(testPod2.Namespace, testPod2.Name)
				Expect(errDelete).NotTo(HaveOccurred())
			}()
			wg.Wait()

			By("Create use csi hostpath pv pod3")
			testPod3, mountPaths3 := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest3", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, false)
			Expect(testPod3).NotTo(BeNil())
			Expect(len(mountPaths3)).To(Equal(1))

			By("Wait pod3 running")
			curPod3, waitPhaseErr3 := f.WaitForPodPhase(testPod3.Namespace, testPod3.Name, v1.PodRunning)
			Expect(waitPhaseErr3).NotTo(HaveOccurred())
			Expect(curPod3).NotTo(BeNil())

			By("Get pod3 csi mount path info")
			capcity3, used3, _ := getCSIPathInfo(f, testPod3.Namespace, testPod3.Name, testPod3.Spec.Containers[0].Name, mountPaths3[0])
			Expect(capcity3).To(Equal(testSize * 2 / 1024)) // capcity is KB
			Expect(used3 <= capcity3).To(Equal(true))

			By("Sleep 20 seconds")
			time.Sleep(20 * time.Second)

			By("Check mount info after pod3 running")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod3.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
			Expect(mountInfo[0].MountInfos[0].PodInfo).To(BeNil())

		}, 100)

	It("should ensure none true csi hostpath pv features.",
		func() {
			var testSize int64 = 10 * 1024 * 1024
			By("Creating a csi hostpath pv test namespace")
			ns, err := f.CreateNamespace("csihostpathnonetruetest", nil, nil)
			Expect(err).NotTo(HaveOccurred())

			By("Creating CSIHostpath test PV")
			pvCSI, errPVCSI := f.CreateHostPathPV("csihostpathnonetruetest", true, testSize, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "none", "io.enndata.user/alpha-pvhostpathquotaforonepod": "true"})
			Expect(errPVCSI).NotTo(HaveOccurred())

			By("Create CSIHostpath test PVC")
			pvcCSI, errPVCCSI := f.CreateHostPathPVC("csihostpathnonetruetest", ns.Name, pvCSI.Name, testSize)
			Expect(errPVCCSI).NotTo(HaveOccurred())

			By("Wait csi hostpath pvc bound")
			pvcCSIBoundErr := f.WaitForPVCStatus(pvcCSI.Namespace, pvcCSI.Name, v1.ClaimBound)
			Expect(pvcCSIBoundErr).NotTo(HaveOccurred())

			By("Create use csi hostpath pv pod")
			testPod, mountPaths := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, true)
			Expect(testPod).NotTo(BeNil())
			Expect(len(mountPaths)).To(Equal(1))

			By("Wait pod running")
			curPod, waitPhaseErr := f.WaitForPodPhase(testPod.Namespace, testPod.Name, v1.PodRunning)
			Expect(waitPhaseErr).NotTo(HaveOccurred())
			Expect(curPod).NotTo(BeNil())

			By("sleep 3 seconds")
			time.Sleep(3 * time.Second)

			By("Get pod csi mount path info")
			capcity, used, _ := getCSIPathInfo(f, testPod.Namespace, testPod.Name, testPod.Spec.Containers[0].Name, mountPaths[0])
			Expect(capcity).To(Equal(testSize / 1024)) // capcity is KB
			Expect(used <= capcity).To(Equal(true))

			By("Wait pv mount info update")
			errWait := wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 0 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info")
			mountInfo, errGetInfo := f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
			Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
			Expect(mountInfo[0].MountInfos[0].PodInfo.Info).To(Equal(fmt.Sprintf("%s:%s:%s", curPod.Namespace, curPod.Name, curPod.UID)))

			By("Create use csi hostpath pv pod2")
			testPod2, mountPaths2 := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest2", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, true)
			Expect(testPod2).NotTo(BeNil())
			Expect(len(mountPaths2)).To(Equal(1))

			By("Wait pod2 running")
			curPod2, waitPhaseErr2 := f.WaitForPodPhase(testPod2.Namespace, testPod2.Name, v1.PodRunning)
			Expect(waitPhaseErr2).NotTo(HaveOccurred())
			Expect(curPod2).NotTo(BeNil())

			By("sleep 3 seconds")
			time.Sleep(3 * time.Second)

			By("Get pod2 csi mount path info")
			capcity2, used2, _ := getCSIPathInfo(f, testPod2.Namespace, testPod2.Name, testPod2.Spec.Containers[0].Name, mountPaths2[0])
			Expect(capcity2).To(Equal(testSize / 1024)) // capcity is KB
			Expect(used2 <= capcity2).To(Equal(true))

			By("Wait pv mount info update")
			errWait = wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 1 || (len(mountInfo) == 1 && len(mountInfo[0].MountInfos) > 1) {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info with 2 pods")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			podInfo := fmt.Sprintf("%s:%s:%s", testPod.Namespace, testPod.Name, testPod.UID)
			podInfo2 := fmt.Sprintf("%s:%s:%s", testPod2.Namespace, testPod2.Name, testPod2.UID)
			if len(mountInfo) == 1 {
				Expect(len(mountInfo[0].MountInfos)).To(Equal(2))
				Expect(curPod.Status.HostIP).To(Equal(curPod2.Status.HostIP))
				Expect(curPod.Status.HostIP).To(Equal(mountInfo[0].NodeName))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
				Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[1].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo || mountInfo[0].MountInfos[1].PodInfo.Info == podInfo).To(Equal(true))
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo2 || mountInfo[0].MountInfos[1].PodInfo.Info == podInfo2).To(Equal(true))
			} else {
				Expect(len(mountInfo)).To(Equal(2))
				Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
				Expect(len(mountInfo[1].MountInfos)).To(Equal(1))
				Expect(mountInfo[0].NodeName == curPod.Status.HostIP || mountInfo[1].NodeName == curPod.Status.HostIP).To(Equal(true))
				Expect(mountInfo[0].NodeName == curPod2.Status.HostIP || mountInfo[1].NodeName == curPod2.Status.HostIP).To(Equal(true))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
				Expect(mountInfo[1].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
				Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[1].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo || mountInfo[0].MountInfos[0].PodInfo.Info == podInfo).To(Equal(true))
				Expect(mountInfo[1].MountInfos[0].PodInfo.Info == podInfo2 || mountInfo[1].MountInfos[0].PodInfo.Info == podInfo2).To(Equal(true))
			}

			By("Scale pv's capcity")
			errScale := f.UpdateHostPathPVCapcity(pvCSI.Name, testSize*2)
			Expect(errScale).NotTo(HaveOccurred())

			By("Wait pv mount info update")
			errWait = wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 1 && len(mountInfo[0].MountInfos) > 0 && mountInfo[0].MountInfos[0].VolumeQuotaSize >= testSize*2 &&
					len(mountInfo[1].MountInfos) > 0 && mountInfo[1].MountInfos[0].VolumeQuotaSize >= testSize*2 {
					return true, nil
				}
				if len(mountInfo) == 1 && len(mountInfo[0].MountInfos) > 1 && mountInfo[0].MountInfos[0].VolumeQuotaSize >= testSize*2 &&
					mountInfo[0].MountInfos[1].VolumeQuotaSize >= testSize*2 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info with 2 pods after scale")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			if len(mountInfo) == 1 {
				Expect(len(mountInfo[0].MountInfos)).To(Equal(2))
				Expect(curPod.Status.HostIP).To(Equal(curPod2.Status.HostIP))
				Expect(curPod.Status.HostIP).To(Equal(mountInfo[0].NodeName))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
				Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[1].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo || mountInfo[0].MountInfos[1].PodInfo.Info == podInfo).To(Equal(true))
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo2 || mountInfo[0].MountInfos[1].PodInfo.Info == podInfo2).To(Equal(true))
			} else {
				Expect(len(mountInfo)).To(Equal(2))
				Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
				Expect(len(mountInfo[1].MountInfos)).To(Equal(1))
				Expect(mountInfo[0].NodeName == curPod.Status.HostIP || mountInfo[1].NodeName == curPod.Status.HostIP).To(Equal(true))
				Expect(mountInfo[0].NodeName == curPod2.Status.HostIP || mountInfo[1].NodeName == curPod2.Status.HostIP).To(Equal(true))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
				Expect(mountInfo[1].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
				Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[1].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo || mountInfo[0].MountInfos[0].PodInfo.Info == podInfo).To(Equal(true))
				Expect(mountInfo[1].MountInfos[0].PodInfo.Info == podInfo2 || mountInfo[1].MountInfos[0].PodInfo.Info == podInfo2).To(Equal(true))
			}

			By("Delete pod2")
			errDelete := f.DeleteAndWaitPodDelete(testPod2.Namespace, testPod2.Name)
			Expect(errDelete).NotTo(HaveOccurred())

			By("Wait pv mount info update")
			errWait = wait.PollImmediate(1*time.Second, 40*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				sum := 0
				for _, mi := range mountInfo {
					sum += len(mi.MountInfos)
				}
				if sum == 1 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			if len(mountInfo) == 1 {
				Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
				Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
				Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info).To(Equal(fmt.Sprintf("%s:%s:%s", curPod.Namespace, curPod.Name, curPod.UID)))
			} else {
				Expect(len(mountInfo)).To(Equal(2))
				if mountInfo[0].NodeName == curPod.Status.HostIP {
					Expect(len(mountInfo[1].MountInfos)).To(Equal(0))
					Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
					Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
					Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
					Expect(mountInfo[0].MountInfos[0].PodInfo.Info).To(Equal(fmt.Sprintf("%s:%s:%s", curPod.Namespace, curPod.Name, curPod.UID)))
				} else {
					Expect(len(mountInfo[0].MountInfos)).To(Equal(0))
					Expect(mountInfo[1].NodeName).To(Equal(curPod.Status.HostIP))
					Expect(len(mountInfo[1].MountInfos)).To(Equal(1))
					Expect(mountInfo[1].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
					Expect(mountInfo[1].MountInfos[0].PodInfo).NotTo(BeNil())
					Expect(mountInfo[1].MountInfos[0].PodInfo.Info).To(Equal(fmt.Sprintf("%s:%s:%s", curPod.Namespace, curPod.Name, curPod.UID)))
				}
			}

			By("Delete pod")
			errDelete = f.DeleteAndWaitPodDelete(testPod.Namespace, testPod.Name)
			Expect(errDelete).NotTo(HaveOccurred())

			By("Wait pv mount info update")
			errWait = wait.PollImmediate(1*time.Second, 40*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				sum := 0
				for _, mi := range mountInfo {
					sum += len(mi.MountInfos)
				}
				if sum == 0 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())
		}, 100)

	It("should ensure keep true csi hostpath pv features.",
		func() {
			var testSize int64 = 10 * 1024 * 1024
			By("Creating a csi hostpath pv test namespace")
			ns, err := f.CreateNamespace("csihostpathkeeptruetest", nil, nil)
			Expect(err).NotTo(HaveOccurred())

			By("Creating CSIHostpath test PV")
			pvCSI, errPVCSI := f.CreateHostPathPV("csihostpathkeeptruetest", true, testSize, map[string]string{"io.enndata.user/alpha-pvhostpathmountpolicy": "keep", "io.enndata.user/alpha-pvhostpathquotaforonepod": "true"})
			Expect(errPVCSI).NotTo(HaveOccurred())

			By("Create CSIHostpath test PVC")
			pvcCSI, errPVCCSI := f.CreateHostPathPVC("csihostpathkeeptruetest", ns.Name, pvCSI.Name, testSize)
			Expect(errPVCCSI).NotTo(HaveOccurred())

			By("Wait csi hostpath pvc bound")
			pvcCSIBoundErr := f.WaitForPVCStatus(pvcCSI.Namespace, pvcCSI.Name, v1.ClaimBound)
			Expect(pvcCSIBoundErr).NotTo(HaveOccurred())

			By("Create use csi hostpath pv pod")
			testPod, mountPaths := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, true)
			Expect(testPod).NotTo(BeNil())
			Expect(len(mountPaths)).To(Equal(1))

			By("Wait pod running")
			curPod, waitPhaseErr := f.WaitForPodPhase(testPod.Namespace, testPod.Name, v1.PodRunning)
			Expect(waitPhaseErr).NotTo(HaveOccurred())
			Expect(curPod).NotTo(BeNil())

			By("sleep 3 seconds")
			time.Sleep(3 * time.Second)

			By("Get pod csi mount path info")
			capcity, used, _ := getCSIPathInfo(f, testPod.Namespace, testPod.Name, testPod.Spec.Containers[0].Name, mountPaths[0])
			Expect(capcity).To(Equal(testSize / 1024)) // capcity is KB
			Expect(used <= capcity).To(Equal(true))

			By("Wait pv mount info update")
			errWait := wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 0 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info")
			mountInfo, errGetInfo := f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize))
			Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
			Expect(mountInfo[0].MountInfos[0].PodInfo.Info).To(Equal(fmt.Sprintf("%s:%s:%s", curPod.Namespace, curPod.Name, curPod.UID)))

			By("Scale pv's capcity")
			errScale := f.UpdateHostPathPVCapcity(pvCSI.Name, testSize*2)
			Expect(errScale).NotTo(HaveOccurred())

			By("Wait pv's mount info update")
			errWait = wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 0 && mountInfo[0].MountInfos[0].VolumeQuotaSize >= testSize*2 {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info again")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
			Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
			Expect(mountInfo[0].MountInfos[0].PodInfo.Info).To(Equal(fmt.Sprintf("%s:%s:%s", curPod.Namespace, curPod.Name, curPod.UID)))

			By("Get pod csi mount path info again")
			capcity, used, _ = getCSIPathInfo(f, testPod.Namespace, testPod.Name, testPod.Spec.Containers[0].Name, mountPaths[0])
			Expect(capcity).To(Equal(testSize * 2 / 1024)) // capcity is KB
			Expect(used <= capcity).To(Equal(true))

			By("Delete pod")
			errDelete := f.DeleteAndWaitPodDelete(testPod.Namespace, testPod.Name)
			Expect(errDelete).NotTo(HaveOccurred())

			By("Create use csi hostpath pv pod2")
			testPod, mountPaths = createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest2", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, true)
			Expect(testPod).NotTo(BeNil())
			Expect(len(mountPaths)).To(Equal(1))

			By("Wait pod2 running")
			curPod, waitPhaseErr = f.WaitForPodPhase(testPod.Namespace, testPod.Name, v1.PodRunning)
			Expect(waitPhaseErr).NotTo(HaveOccurred())
			Expect(curPod).NotTo(BeNil())

			By("Get pod2 csi mount path info")
			capcity, used, _ = getCSIPathInfo(f, testPod.Namespace, testPod.Name, testPod.Spec.Containers[0].Name, mountPaths[0])
			Expect(capcity).To(Equal(testSize * 2 / 1024)) // capcity is KB
			Expect(used <= capcity).To(Equal(true))

			By("Wait pv's mount info update after pod2 created")
			errWait = wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 0 && mountInfo[0].MountInfos[0].PodInfo != nil &&
					mountInfo[0].MountInfos[0].PodInfo.Info == fmt.Sprintf("%s:%s:%s", testPod.Namespace, testPod.Name, testPod.UID) {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info of pod2")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			Expect(len(mountInfo)).To(Equal(1))
			Expect(mountInfo[0].NodeName).To(Equal(curPod.Status.HostIP))
			Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
			Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
			Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
			Expect(mountInfo[0].MountInfos[0].PodInfo.Info).To(Equal(fmt.Sprintf("%s:%s:%s", testPod.Namespace, testPod.Name, testPod.UID)))

			By("Create use csi hostpath pv pod3")
			testPod3, mountPaths3 := createPodUseHostPathCSI(f, ns.Name, "hostpathcsitest3", []*v1.PersistentVolumeClaim{pvcCSI}, testSize*3, true)
			Expect(testPod3).NotTo(BeNil())
			Expect(len(mountPaths3)).To(Equal(1))

			By("Wait pod3 running")
			curPod3, waitPhaseErr3 := f.WaitForPodPhase(testPod3.Namespace, testPod3.Name, v1.PodRunning)
			Expect(waitPhaseErr3).NotTo(HaveOccurred())
			Expect(curPod3).NotTo(BeNil())

			By("Get pod3 csi mount path info")
			capcity3, used3, _ := getCSIPathInfo(f, testPod3.Namespace, testPod3.Name, testPod3.Spec.Containers[0].Name, mountPaths3[0])
			Expect(capcity3).To(Equal(testSize * 2 / 1024)) // capcity is KB
			Expect(used3 <= capcity3).To(Equal(true))

			By("Wait pv's mount info update after pod3 created")
			errWait = wait.PollImmediate(1*time.Second, 30*time.Second, func() (bool, error) {
				mountInfo, err := f.GetHostpathPVMountInfos(pvCSI.Name)
				if err != nil {
					return false, err
				}
				if len(mountInfo) > 1 || (len(mountInfo) == 1 && len(mountInfo[0].MountInfos) > 1) {
					return true, nil
				}
				return false, nil
			})
			Expect(errWait).NotTo(HaveOccurred())

			By("Check mount info with 2 pods")
			mountInfo, errGetInfo = f.GetHostpathPVMountInfos(pvCSI.Name)
			Expect(errGetInfo).NotTo(HaveOccurred())
			podInfo := fmt.Sprintf("%s:%s:%s", testPod.Namespace, testPod.Name, testPod.UID)
			podInfo3 := fmt.Sprintf("%s:%s:%s", testPod3.Namespace, testPod3.Name, testPod3.UID)
			if len(mountInfo) == 1 {
				Expect(len(mountInfo[0].MountInfos)).To(Equal(2))
				Expect(curPod.Status.HostIP).To(Equal(curPod3.Status.HostIP))
				Expect(curPod.Status.HostIP).To(Equal(mountInfo[0].NodeName))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
				Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[1].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo || mountInfo[0].MountInfos[1].PodInfo.Info == podInfo).To(Equal(true))
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo3 || mountInfo[0].MountInfos[1].PodInfo.Info == podInfo3).To(Equal(true))
			} else {
				Expect(len(mountInfo)).To(Equal(2))
				Expect(len(mountInfo[0].MountInfos)).To(Equal(1))
				Expect(len(mountInfo[1].MountInfos)).To(Equal(1))
				Expect(mountInfo[0].NodeName == curPod.Status.HostIP || mountInfo[1].NodeName == curPod.Status.HostIP).To(Equal(true))
				Expect(mountInfo[0].NodeName == curPod3.Status.HostIP || mountInfo[1].NodeName == curPod3.Status.HostIP).To(Equal(true))
				Expect(mountInfo[0].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
				Expect(mountInfo[1].MountInfos[0].VolumeQuotaSize).To(Equal(testSize * 2))
				Expect(mountInfo[0].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[1].MountInfos[0].PodInfo).NotTo(BeNil())
				Expect(mountInfo[0].MountInfos[0].PodInfo.Info == podInfo || mountInfo[0].MountInfos[0].PodInfo.Info == podInfo).To(Equal(true))
				Expect(mountInfo[1].MountInfos[0].PodInfo.Info == podInfo3 || mountInfo[1].MountInfos[0].PodInfo.Info == podInfo3).To(Equal(true))
			}
		}, 100)
})

type MyBuf []byte

func (mb *MyBuf) Write(p []byte) (n int, err error) {
	*mb = append(*mb, p...)
	return len(p), nil
}

func getCSIPathInfo(f *framework.Framework, namespace, name, container, mountPath string) (capcity, used, avail int64) {
	req := f.ClientSet.CoreV1().RESTClient().Post().Resource("pods").Name(name).Namespace(namespace).SubResource("exec").Param("container", container)
	req.VersionedParams(&api.PodExecOptions{
		Container: container,
		Command:   []string{"df"},
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, legacyscheme.ParameterCodec)
	//	var output bytes.Buffer
	//output := bytes.NewBuffer([]byte{})
	output := MyBuf{}
	exec, errExec := remotecommand.NewSPDYExecutor(f.GetClientSetConfig(), "POST", req.URL())
	Expect(errExec).NotTo(HaveOccurred())

	errExec = exec.Stream(remotecommand.StreamOptions{
		Stdin:             os.Stdin,
		Stdout:            &output,
		Stderr:            &output,
		Tty:               false,
		TerminalSizeQueue: nil,
	})
	time.Sleep(1 * time.Second)
	Expect(errExec).NotTo(HaveOccurred())

	read := bytes.NewBuffer([]byte(output))

	for {
		line, err := read.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			Expect(err).NotTo(HaveOccurred())
		}
		if strings.Contains(line, mountPath) {
			var dev string
			fmt.Sscanf(line, "%s %d %d %d", &dev, &capcity, &used, &avail)
		}
	}
	return capcity, used, avail
}

func createPodUseHostPathCSI(f *framework.Framework, namespace, name string, pvcs []*v1.PersistentVolumeClaim, size int64, ddWrite bool) (*v1.Pod, []string) {
	var terminationGracePeriodSeconds int64 = 0
	volumes := []v1.Volume{}
	volumeMounts := []v1.VolumeMount{}
	mountPaths := []string{}
	ddCmd := ""
	if pvcs != nil && len(pvcs) > 0 {
		for i, pvc := range pvcs {
			volumes = append(volumes, v1.Volume{
				Name: pvc.Name,
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvc.Name,
					},
				},
			})
			mp := fmt.Sprintf("/mnt/data%d", i+1)
			mountPaths = append(mountPaths, mp)
			volumeMounts = append(volumeMounts, v1.VolumeMount{
				Name:      pvc.Name,
				MountPath: mp,
			})
			ddCmd += fmt.Sprintf("dd if=/dev/zero of=%s/data bs=1024 count=%d;", mp, size/1024)
		}
	}
	if ddWrite == false {
		ddCmd = ""
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
			Volumes: volumes,
			Containers: []v1.Container{
				{
					Name:         "test",
					Image:        imageutils.GetAlpineImageName(),
					Command:      []string{"sh", "-c"},
					Args:         []string{fmt.Sprintf("while true; do %s sleep 1; done", ddCmd)},
					VolumeMounts: volumeMounts,
				},
			},
		},
	}
	podCreate, err := f.ClientSet.CoreV1().Pods(namespace).Create(pod)
	Expect(err).NotTo(HaveOccurred())

	return podCreate, mountPaths
}
