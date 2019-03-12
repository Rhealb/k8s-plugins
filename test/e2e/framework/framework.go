package framework

import (
	"fmt"
	"strings"
	"time"

	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

const (
	DefaultNamespaceDeletionTimeout = 10 * time.Minute
	DefaultPVCDeletionTimeout       = 10 * time.Second
	DefaultPVDeletionTimeout        = 1 * time.Minute
	DefaultPVCBoundTimeout          = 1 * time.Minute
	DefaultPodSchedulerTimeout      = 1 * time.Minute
	DefaultPodRunningTimeout        = 2 * time.Minute
	DefaultPVUpdateTryTimes         = 5
	DefaultPodDeleteTimeOut         = 1 * time.Minute
)

type FrameworkOptions struct {
	ClientQPS    float32
	ClientBurst  int
	GroupVersion *schema.GroupVersion
}

type TestDataSummary interface {
	SummaryKind() string
	PrintHumanReadable() string
	PrintJSON() string
}

type Framework struct {
	BaseName string

	ClientSet clientset.Interface

	//	InternalClientset *internalclientset.Clientset
	DynamicClient dynamic.Interface
	//	SkipNamespaceCreation    bool            // Whether to skip creating a namespace
	Namespace                *v1.Namespace   // Every test has at least one namespace unless creation is skipped
	namespacesToDelete       []*v1.Namespace // Some tests have more than one.
	pvToDelete               []*v1.PersistentVolume
	pvcToDelete              []*v1.PersistentVolumeClaim
	NamespaceDeletionTimeout time.Duration
	PVDeletionTimeout        time.Duration
	PVCDeletionTimeout       time.Duration

	cleanupHandle CleanupActionHandle
	TestSummaries []TestDataSummary
	// configuration for framework's client
	Options FrameworkOptions
	config  *restclient.Config
}

func NewDefaultFramework(baseName string) *Framework {
	options := FrameworkOptions{
		ClientQPS:   20,
		ClientBurst: 50,
	}
	return NewFramework(baseName, options, nil)
}

func NewFramework(baseName string, options FrameworkOptions, client clientset.Interface) *Framework {
	f := &Framework{
		BaseName:  baseName,
		Options:   options,
		ClientSet: client,
	}

	BeforeEach(f.BeforeEach)
	AfterEach(f.AfterEach)

	return f
}

func (f *Framework) GetClientSetConfig() *restclient.Config {
	if f.config != nil {
		return f.config
	}
	return nil
}

func (f *Framework) BeforeEach() {
	f.cleanupHandle = AddCleanupAction(f.AfterEach)
	if f.ClientSet == nil {
		By("Creating a kubernetes client")
		config, err := LoadConfig()
		Expect(err).NotTo(HaveOccurred())
		config.QPS = f.Options.ClientQPS
		config.Burst = f.Options.ClientBurst
		f.config = config
		if f.Options.GroupVersion != nil {
			config.GroupVersion = f.Options.GroupVersion
		}
		if TestContext.KubeAPIContentType != "" {
			config.ContentType = TestContext.KubeAPIContentType
		}
		f.ClientSet, err = clientset.NewForConfig(config)
		Expect(err).NotTo(HaveOccurred())
		f.DynamicClient, err = dynamic.NewForConfig(config)
		Expect(err).NotTo(HaveOccurred())
	}
}

func (f *Framework) AfterEach() {
	f.cleanupHandle = AddCleanupAction(f.AfterEach)
	defer func() {
		nsDeletionErrors := map[string]error{}
		pvDeletionErrors := map[string]error{}
		pvcDeletionErrors := map[string]error{}
		// Whether to delete namespace is determined by 3 factors: delete-namespace flag, delete-namespace-on-failure flag and the test result
		// if delete-namespace set to false, namespace will always be preserved.
		// if delete-namespace is true and delete-namespace-on-failure is false, namespace will be preserved if test failed.
		if TestContext.DeleteNamespace && (TestContext.DeleteNamespaceOnFailure || !CurrentGinkgoTestDescription().Failed) {
			for _, ns := range f.namespacesToDelete {
				By(fmt.Sprintf("Destroying namespace %q for this suite.", ns.Name))
				timeout := DefaultNamespaceDeletionTimeout
				if f.NamespaceDeletionTimeout != 0 {
					timeout = f.NamespaceDeletionTimeout
				}
				if err := deleteNS(f.ClientSet, f.DynamicClient, ns.Name, timeout); err != nil {
					if !apierrors.IsNotFound(err) {
						nsDeletionErrors[ns.Name] = err
					} else {
						Logf("Namespace %v was already deleted", ns.Name)
					}
				}
			}
		} else {
			if !TestContext.DeleteNamespace {
				Logf("Found DeleteNamespace=false, skipping namespace deletion!")
			} else {
				Logf("Found DeleteNamespaceOnFailure=false and current test failed, skipping namespace deletion!")
			}
		}

		if TestContext.DeletePV && (TestContext.DeletePVOnFailure || !CurrentGinkgoTestDescription().Failed) {
			for _, pvc := range f.pvcToDelete {
				By(fmt.Sprintf("Destroying pvc %q for this suite.", pvc.Name))
				timeout := DefaultPVCDeletionTimeout
				if f.PVCDeletionTimeout != 0 {
					timeout = f.PVCDeletionTimeout
				}
				if err := deletePVC(f.ClientSet, f.DynamicClient, pvc.Namespace, pvc.Name, timeout); err != nil {
					if !apierrors.IsNotFound(err) {
						pvcDeletionErrors[pvc.Name] = err
					} else {
						Logf("PVC %v was already deleted", pvc.Name)
					}
				}
			}
			for _, pv := range f.pvToDelete {
				By(fmt.Sprintf("Destroying pv %q for this suite.", pv.Name))
				timeout := DefaultPVDeletionTimeout
				if f.PVDeletionTimeout != 0 {
					timeout = f.PVDeletionTimeout
				}
				if err := deletePV(f.ClientSet, f.DynamicClient, pv.Name, timeout); err != nil {
					if !apierrors.IsNotFound(err) {
						pvDeletionErrors[pv.Name] = err
					} else {
						Logf("PV %v was already deleted", pv.Name)
					}
				}
			}
		}

		// Paranoia-- prevent reuse!
		f.Namespace = nil
		f.ClientSet = nil
		f.namespacesToDelete = nil

		// if we had errors deleting, report them now.
		if len(nsDeletionErrors) != 0 || len(pvcDeletionErrors) != 0 || len(pvDeletionErrors) != 0 {
			messages := []string{}
			for namespaceKey, namespaceErr := range nsDeletionErrors {
				messages = append(messages, fmt.Sprintf("Couldn't delete ns: %q: %s (%#v)", namespaceKey, namespaceErr, namespaceErr))
			}
			for pvcKey, pvcErr := range pvcDeletionErrors {
				messages = append(messages, fmt.Sprintf("Couldn't delete pvc: %q: %s (%#v)", pvcKey, pvcErr, pvcErr))
			}
			for pvKey, pvErr := range pvDeletionErrors {
				messages = append(messages, fmt.Sprintf("Couldn't delete pv: %q: %s (%#v)", pvKey, pvErr, pvErr))
			}
			Failf(strings.Join(messages, ","))
		}
	}()
	PrintSummaries(f.TestSummaries, f.BaseName)
	//	if err := AllNodesReady(f.ClientSet, 3*time.Minute); err != nil {
	//		Failf("All nodes should be ready after test, %v", err)
	//	}
}

func (f *Framework) CreateHostPathPV(baseName string, isCSI bool, size int64, anns map[string]string) (*v1.PersistentVolume, error) {
	createTestingHostPathPVFn := TestContext.CreateTestingHostPathPV
	if createTestingHostPathPVFn == nil {
		createTestingHostPathPVFn = CreateTestingHostPathPV
	}
	pv, err := createTestingHostPathPVFn(baseName, f.ClientSet, isCSI, size, anns)
	f.AddPVToDelete(pv)
	return pv, err
}

func (f *Framework) GetHostpathPVMountInfos(pvName string) (hostpath.HostPathPVMountInfoList, error) {
	return GetHostpathPVMountInfos(f.ClientSet, pvName)
}

func (f *Framework) UpdateHostPathPVCapcity(pvName string, newCapcity int64) error {
	return UpdateHostPathPVCapcity(f.ClientSet, pvName, newCapcity, DefaultPVUpdateTryTimes)
}

func (f *Framework) WaitForPVCStatus(pvcNS, pvcName string, phase v1.PersistentVolumeClaimPhase) error {
	return WaitForPVCStatus(f.ClientSet, pvcNS, pvcName, DefaultPVCBoundTimeout, phase)
}

func (f *Framework) WaitForPodScheduled(podNs, podName string) (string, error) {
	return WaitForPodScheduled(f.ClientSet, podNs, podName, DefaultPodSchedulerTimeout)
}

func (f *Framework) WaitForPodPhase(podNs, podName string, phase v1.PodPhase) (*v1.Pod, error) {
	return WaitForPodPhase(f.ClientSet, podNs, podName, phase, DefaultPodRunningTimeout)
}

func (f *Framework) DeleteAndWaitPodDelete(podNs, podName string) error {
	return DeleteAndWaitPodDelete(f.ClientSet, podNs, podName, DefaultPodDeleteTimeOut)
}

func (f *Framework) CreateHostPathPVC(baseName, pvcNS, volumeName string, size int64) (*v1.PersistentVolumeClaim, error) {
	createTestingHostPathPVCFn := TestContext.CreateTestingHostPathPVC
	if createTestingHostPathPVCFn == nil {
		createTestingHostPathPVCFn = CreateTestingHostPathPVC
	}
	pvc, err := createTestingHostPathPVCFn(baseName, f.ClientSet, size, pvcNS, volumeName)
	f.AddPVCToDelete(pvc)
	return pvc, err
}

func (f *Framework) CreateNamespace(baseName string, labels map[string]string, anns map[string]string) (*v1.Namespace, error) {
	createTestingNS := TestContext.CreateTestingNS
	if createTestingNS == nil {
		createTestingNS = CreateTestingNS
	}
	ns, err := createTestingNS(baseName, f.ClientSet, labels, anns)
	// check ns instead of err to see if it's nil as we may
	// fail to create serviceAccount in it.
	f.AddNamespacesToDelete(ns)

	return ns, err
}

func (f *Framework) AddNamespacesToDelete(namespaces ...*v1.Namespace) {
	for _, ns := range namespaces {
		if ns == nil {
			continue
		}
		f.namespacesToDelete = append(f.namespacesToDelete, ns)

	}
}

func (f *Framework) AddPVToDelete(pvs ...*v1.PersistentVolume) {
	for _, pv := range pvs {
		if pv == nil {
			continue
		}
		f.pvToDelete = append(f.pvToDelete, pv)

	}
}

func (f *Framework) AddPVCToDelete(pvcs ...*v1.PersistentVolumeClaim) {
	for _, pvc := range pvcs {
		if pvc == nil {
			continue
		}
		f.pvcToDelete = append(f.pvcToDelete, pvc)

	}
}

func EnndataDescribe(text string, body func()) bool {
	return Describe("[enndata.cn] "+text, body)
}
