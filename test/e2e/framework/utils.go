package framework

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath"
	"github.com/Rhealb/csi-plugin/hostpathpv/pkg/hostpath/xfsquotamanager/common"
	"github.com/k8s-plugins/test/e2e/framework/ginkgowrapper"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
	"k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/client/conditions"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	testutils "k8s.io/kubernetes/test/utils"
)

const (
	Poll                = 2 * time.Second
	PodStartTimeout     = 1 * time.Minute
	slowPodStartTimeout = 2 * time.Minute
)

type CreateTestingNSFn func(baseName string, c clientset.Interface, labels map[string]string, anns map[string]string) (*v1.Namespace, error)
type CreateTestingHostPathPVFn func(baseName string, c clientset.Interface, isCSIPV bool, size int64, anns map[string]string) (*v1.PersistentVolume, error)
type CreateTestingHostPathPVCFn func(baseName string, c clientset.Interface, size int64, pvcNS, volumeName string) (*v1.PersistentVolumeClaim, error)

type podCondition func(pod *v1.Pod) (bool, error)

func nowStamp() string {
	return time.Now().Format(time.StampMilli)
}

func log(level string, format string, args ...interface{}) {
	fmt.Fprintf(GinkgoWriter, nowStamp()+": "+level+": "+format+"\n", args...)
}

func Logf(format string, args ...interface{}) {
	log("INFO", format, args...)
}

func Failf(format string, args ...interface{}) {
	FailfWithOffset(1, format, args...)
}

func FailfWithOffset(offset int, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log("INFO", msg)
	ginkgowrapper.Fail(nowStamp()+": "+msg, 1+offset)
}

func RestclientConfig(kubeContext string) (*clientcmdapi.Config, error) {
	Logf(">>> kubeConfig: %s", TestContext.KubeConfig)
	if TestContext.KubeConfig == "" {
		return nil, fmt.Errorf("KubeConfig must be specified to load client config")
	}
	c, err := clientcmd.LoadFromFile(TestContext.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("error loading KubeConfig: %v", err.Error())
	}
	if kubeContext != "" {
		Logf(">>> kubeContext: %s", kubeContext)
		c.CurrentContext = kubeContext
	}
	return c, nil
}

func LoadConfig() (*restclient.Config, error) {
	c, err := RestclientConfig(TestContext.KubeContext)
	if err != nil {
		if TestContext.KubeConfig == "" {
			return restclient.InClusterConfig()
		} else {
			return nil, err
		}
	}

	return clientcmd.NewDefaultClientConfig(*c, &clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: TestContext.Host}}).ClientConfig()
}

func LoadInternalClientset() (*internalclientset.Clientset, error) {
	config, err := LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err.Error())
	}
	return internalclientset.NewForConfig(config)
}

func LoadClientset() (*clientset.Clientset, error) {
	config, err := LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err.Error())
	}
	return clientset.NewForConfig(config)
}

// unique identifier of the e2e run
var RunId = NewUUID()

var uuidLock sync.Mutex
var lastUUID uuid.UUID

func NewUUID() types.UID {
	uuidLock.Lock()
	defer uuidLock.Unlock()
	result := uuid.NewUUID()
	// The UUID package is naive and can generate identical UUIDs if the
	// time interval is quick enough.
	// The UUID uses 100 ns increments so it's short enough to actively
	// wait for a new value.
	for uuid.Equal(lastUUID, result) == true {
		result = uuid.NewUUID()
	}
	lastUUID = result
	return types.UID(result.String())
}

// hasRemainingContent checks if there is remaining content in the namespace via API discovery
func hasRemainingContent(c clientset.Interface, dynamicClient dynamic.Interface, namespace string) (bool, error) {
	// some tests generate their own framework.Client rather than the default
	// TODO: ensure every test call has a configured dynamicClient
	if dynamicClient == nil {
		return false, nil
	}

	// find out what content is supported on the server
	// Since extension apiserver is not always available, e.g. metrics server sometimes goes down,
	// add retry here.
	resources, err := waitForServerPreferredNamespacedResources(c.Discovery(), 30*time.Second)
	if err != nil {
		return false, err
	}
	groupVersionResources, err := discovery.GroupVersionResources(resources)
	if err != nil {
		return false, err
	}

	// TODO: temporary hack for https://github.com/kubernetes/kubernetes/issues/31798
	ignoredResources := sets.NewString("bindings")

	contentRemaining := false

	// dump how many of resource type is on the server in a log.
	for gvr := range groupVersionResources {
		// get a client for this group version...
		dynamicClient := dynamicClient.Resource(gvr).Namespace(namespace)
		if err != nil {
			// not all resource types support list, so some errors here are normal depending on the resource type.
			Logf("namespace: %s, unable to get client - gvr: %v, error: %v", namespace, gvr, err)
			continue
		}
		// get the api resource
		apiResource := metav1.APIResource{Name: gvr.Resource, Namespaced: true}
		if ignoredResources.Has(gvr.Resource) {
			Logf("namespace: %s, resource: %s, ignored listing per whitelist", namespace, apiResource.Name)
			continue
		}
		unstructuredList, err := dynamicClient.List(metav1.ListOptions{})
		if err != nil {
			// not all resources support list, so we ignore those
			if apierrs.IsMethodNotSupported(err) || apierrs.IsNotFound(err) || apierrs.IsForbidden(err) {
				continue
			}
			// skip unavailable servers
			if apierrs.IsServiceUnavailable(err) {
				continue
			}
			return false, err
		}
		if len(unstructuredList.Items) > 0 {
			Logf("namespace: %s, resource: %s, items remaining: %v", namespace, apiResource.Name, len(unstructuredList.Items))
			contentRemaining = true
		}
	}
	return contentRemaining, nil
}

// deleteNS deletes the provided namespace, waits for it to be completely deleted, and then checks
// whether there are any pods remaining in a non-terminating state.
func deleteNS(c clientset.Interface, dynamicClient dynamic.Interface, namespace string, timeout time.Duration) error {
	startTime := time.Now()
	if err := c.CoreV1().Namespaces().Delete(namespace, nil); err != nil {
		return err
	}

	// wait for namespace to delete or timeout.
	err := wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		if _, err := c.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{}); err != nil {
			if apierrs.IsNotFound(err) {
				return true, nil
			}
			Logf("Error while waiting for namespace to be terminated: %v", err)
			return false, nil
		}
		return false, nil
	})

	// verify there is no more remaining content in the namespace
	remainingContent, cerr := hasRemainingContent(c, dynamicClient, namespace)
	if cerr != nil {
		return cerr
	}

	// if content remains, let's dump information about the namespace, and system for flake debugging.
	remainingPods := 0
	missingTimestamp := 0
	if remainingContent {
		// log information about namespace, and set of namespaces in api server to help flake detection
		logNamespace(c, namespace)
		logNamespaces(c, namespace)

		// if we can, check if there were pods remaining with no timestamp.
		remainingPods, missingTimestamp, _ = countRemainingPods(c, namespace)
	}

	// a timeout waiting for namespace deletion happened!
	if err != nil {
		// some content remains in the namespace
		if remainingContent {
			// pods remain
			if remainingPods > 0 {
				if missingTimestamp != 0 {
					// pods remained, but were not undergoing deletion (namespace controller is probably culprit)
					return fmt.Errorf("namespace %v was not deleted with limit: %v, pods remaining: %v, pods missing deletion timestamp: %v", namespace, err, remainingPods, missingTimestamp)
				}
				// but they were all undergoing deletion (kubelet is probably culprit, check NodeLost)
				return fmt.Errorf("namespace %v was not deleted with limit: %v, pods remaining: %v", namespace, err, remainingPods)
			}
			// other content remains (namespace controller is probably screwed up)
			return fmt.Errorf("namespace %v was not deleted with limit: %v, namespaced content other than pods remain", namespace, err)
		}
		// no remaining content, but namespace was not deleted (namespace controller is probably wedged)
		return fmt.Errorf("namespace %v was not deleted with limit: %v, namespace is empty but is not yet removed", namespace, err)
	}
	Logf("namespace %v deletion completed in %s", namespace, time.Since(startTime))
	return nil
}

func deletePVC(c clientset.Interface, dynamicClient dynamic.Interface, pvcName, pvcNS string, timeout time.Duration) error {
	startTime := time.Now()
	if err := c.CoreV1().PersistentVolumeClaims(pvcNS).Delete(pvcName, nil); err != nil {
		return err
	}

	// wait for pvc to delete or timeout.
	err := wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		if _, err := c.CoreV1().PersistentVolumeClaims(pvcNS).Get(pvcName, metav1.GetOptions{}); err != nil {
			if apierrs.IsNotFound(err) {
				return true, nil
			}
			Logf("Error while waiting for pvc to be terminated: %v", err)
			return false, nil
		}
		return false, nil
	})

	// a timeout waiting for pvc deletion happened!
	if err != nil {
		return fmt.Errorf("pvc %v/%v was delete err:%v", pvcNS, pvcName, err)
	}
	Logf("pvc %v/%v deletion completed in %s", pvcNS, pvcName, time.Since(startTime))
	return nil
}

func deletePV(c clientset.Interface, dynamicClient dynamic.Interface, pvName string, timeout time.Duration) error {
	startTime := time.Now()
	if err := c.CoreV1().PersistentVolumes().Delete(pvName, nil); err != nil {
		return err
	}

	// wait for pvc to delete or timeout.
	err := wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		if _, err := c.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{}); err != nil {
			if apierrs.IsNotFound(err) {
				return true, nil
			}
			Logf("Error while waiting for pv to be terminated: %v", err)
			return false, nil
		}
		return false, nil
	})

	// a timeout waiting for pvc deletion happened!
	if err != nil {
		return fmt.Errorf("pv %v was delete err:%v", pvName, err)
	}
	Logf("pv %v deletion completed in %s", pvName, time.Since(startTime))
	return nil
}

// logNamespaces logs the number of namespaces by phase
// namespace is the namespace the test was operating against that failed to delete so it can be grepped in logs
func logNamespaces(c clientset.Interface, namespace string) {
	namespaceList, err := c.CoreV1().Namespaces().List(metav1.ListOptions{})
	if err != nil {
		Logf("namespace: %v, unable to list namespaces: %v", namespace, err)
		return
	}

	numActive := 0
	numTerminating := 0
	for _, namespace := range namespaceList.Items {
		if namespace.Status.Phase == v1.NamespaceActive {
			numActive++
		} else {
			numTerminating++
		}
	}
	Logf("namespace: %v, total namespaces: %v, active: %v, terminating: %v", namespace, len(namespaceList.Items), numActive, numTerminating)
}

// logNamespace logs detail about a namespace
func logNamespace(c clientset.Interface, namespace string) {
	ns, err := c.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{})
	if err != nil {
		if apierrs.IsNotFound(err) {
			Logf("namespace: %v no longer exists", namespace)
			return
		}
		Logf("namespace: %v, unable to get namespace due to error: %v", namespace, err)
		return
	}
	Logf("namespace: %v, DeletionTimetamp: %v, Finalizers: %v, Phase: %v", ns.Name, ns.DeletionTimestamp, ns.Spec.Finalizers, ns.Status.Phase)
}

// countRemainingPods queries the server to count number of remaining pods, and number of pods that had a missing deletion timestamp.
func countRemainingPods(c clientset.Interface, namespace string) (int, int, error) {
	// check for remaining pods
	pods, err := c.CoreV1().Pods(namespace).List(metav1.ListOptions{})
	if err != nil {
		return 0, 0, err
	}

	// nothing remains!
	if len(pods.Items) == 0 {
		return 0, 0, nil
	}

	// stuff remains, log about it
	logPodStates(pods.Items)

	// check if there were any pods with missing deletion timestamp
	numPods := len(pods.Items)
	missingTimestamp := 0
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp == nil {
			missingTimestamp++
		}
	}
	return numPods, missingTimestamp, nil
}

func GetHostpathPVMountInfos(c clientset.Interface, pvName string) (hostpath.HostPathPVMountInfoList, error) {
	curPV, err := c.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{})
	if err != nil {
		return hostpath.HostPathPVMountInfoList{}, err
	}
	if curPV.Annotations == nil || curPV.Annotations[common.PVVolumeHostPathMountNode] == "" {
		return hostpath.HostPathPVMountInfoList{}, nil
	}
	mountList := hostpath.HostPathPVMountInfoList{}
	errUmarshal := json.Unmarshal([]byte(curPV.Annotations[common.PVVolumeHostPathMountNode]), &mountList)
	if errUmarshal != nil {
		return mountList, errUmarshal
	}
	return mountList, nil
}

func UpdateHostPathPVCapcity(c clientset.Interface, pvName string, newCapcity int64, tryTime int) error {
	Logf("Start update hostpath pv %s capcity to %d try %d times", pvName, newCapcity, tryTime)
	for i := 0; i < tryTime; i++ {
		if i > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		curPV, err := c.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{})
		if err != nil {
			if apierrs.IsNotFound(err) {
				Logf("PV %q not found.", pvName)
				return err
			}
			Logf("Get PV %q  Error: %v", pvName, err)
			continue
		}
		if curPV.Annotations == nil {
			curPV.Annotations = map[string]string{}
		}
		curPV.Annotations[common.PVHostPathCapacityAnn] = fmt.Sprintf("%d", newCapcity)
		_, errUpdate := c.CoreV1().PersistentVolumes().Update(curPV)
		if errUpdate != nil {
			Logf("update pv %s err:%v", pvName, errUpdate)
			continue
		}
		return nil
	}
	return fmt.Errorf("Gave up update pv %s after try %d times", pvName, tryTime)
}

func WaitForPVCStatus(c clientset.Interface, pvcNS, pvcName string, timeout time.Duration, phase v1.PersistentVolumeClaimPhase) error {
	Logf("Waiting up to %v for pvc %q in namespace %q to be %q", timeout, pvcName, pvcNS, phase)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		pvc, err := c.CoreV1().PersistentVolumeClaims(pvcNS).Get(pvcName, metav1.GetOptions{})
		if err != nil {
			if apierrs.IsNotFound(err) {
				Logf("PVC %q in namespace %q not found. Error: %v", pvcName, pvcNS, err)
				return err
			}
			Logf("Get PVC %q in namespace %q failed, ignoring for %v. Error: %v", pvcName, pvcNS, Poll, err)
			continue
		}
		// log now so that current pod info is reported before calling `condition()`
		Logf("PVC %q: Phase=%q, Elapsed: %v",
			pvcName, pvc.Status.Phase, time.Since(start))
		if phase == pvc.Status.Phase {
			Logf("PVC %q in namespace %q satisfied phase %q", pvcName, pvcNS, phase)
			return nil
		}
	}
	return fmt.Errorf("Gave up after waiting %v for PVC %q to be %q", timeout, pvcName, phase)
}

func waitForServerPreferredNamespacedResources(d discovery.DiscoveryInterface, timeout time.Duration) ([]*metav1.APIResourceList, error) {
	Logf("Waiting up to %v for server preferred namespaced resources to be successfully discovered", timeout)
	var resources []*metav1.APIResourceList
	if err := wait.PollImmediate(Poll, timeout, func() (bool, error) {
		var err error
		resources, err = d.ServerPreferredNamespacedResources()
		if err == nil || isDynamicDiscoveryError(err) {
			return true, nil
		}
		if !discovery.IsGroupDiscoveryFailedError(err) {
			return false, err
		}
		Logf("Error discoverying server preferred namespaced resources: %v, retrying in %v.", err, Poll)
		return false, nil
	}); err != nil {
		return nil, err
	}
	return resources, nil
}

func logPodStates(pods []v1.Pod) {
	// Find maximum widths for pod, node, and phase strings for column printing.
	maxPodW, maxNodeW, maxPhaseW, maxGraceW := len("POD"), len("NODE"), len("PHASE"), len("GRACE")
	for i := range pods {
		pod := &pods[i]
		if len(pod.ObjectMeta.Name) > maxPodW {
			maxPodW = len(pod.ObjectMeta.Name)
		}
		if len(pod.Spec.NodeName) > maxNodeW {
			maxNodeW = len(pod.Spec.NodeName)
		}
		if len(pod.Status.Phase) > maxPhaseW {
			maxPhaseW = len(pod.Status.Phase)
		}
	}
	// Increase widths by one to separate by a single space.
	maxPodW++
	maxNodeW++
	maxPhaseW++
	maxGraceW++

	// Log pod info. * does space padding, - makes them left-aligned.
	Logf("%-[1]*[2]s %-[3]*[4]s %-[5]*[6]s %-[7]*[8]s %[9]s",
		maxPodW, "POD", maxNodeW, "NODE", maxPhaseW, "PHASE", maxGraceW, "GRACE", "CONDITIONS")
	for _, pod := range pods {
		grace := ""
		if pod.DeletionGracePeriodSeconds != nil {
			grace = fmt.Sprintf("%ds", *pod.DeletionGracePeriodSeconds)
		}
		Logf("%-[1]*[2]s %-[3]*[4]s %-[5]*[6]s %-[7]*[8]s %[9]s",
			maxPodW, pod.ObjectMeta.Name, maxNodeW, pod.Spec.NodeName, maxPhaseW, pod.Status.Phase, maxGraceW, grace, pod.Status.Conditions)
	}
	Logf("") // Final empty line helps for readability.
}

func isDynamicDiscoveryError(err error) bool {
	if !discovery.IsGroupDiscoveryFailedError(err) {
		return false
	}
	discoveryErr := err.(*discovery.ErrGroupDiscoveryFailed)
	for gv := range discoveryErr.Groups {
		switch gv.Group {
		case "mygroup.example.com":
			// custom_resource_definition
			// garbage_collector
		case "wardle.k8s.io":
			// aggregator
		case "metrics.k8s.io":
			// aggregated metrics server add-on, no persisted resources
		default:
			Logf("discovery error for unexpected group: %#v", gv)
			return false
		}
	}
	return true
}

func PrintSummaries(summaries []TestDataSummary, testBaseName string) {
	now := time.Now()
	for i := range summaries {
		Logf("Printing summary: %v", summaries[i].SummaryKind())
		switch TestContext.OutputPrintType {
		case "hr":
			if TestContext.ReportDir == "" {
				Logf(summaries[i].PrintHumanReadable())
			} else {
				// TODO: learn to extract test name and append it to the kind instead of timestamp.
				filePath := path.Join(TestContext.ReportDir, summaries[i].SummaryKind()+"_"+testBaseName+"_"+now.Format(time.RFC3339)+".txt")
				if err := ioutil.WriteFile(filePath, []byte(summaries[i].PrintHumanReadable()), 0644); err != nil {
					Logf("Failed to write file %v with test performance data: %v", filePath, err)
				}
			}
		case "json":
			fallthrough
		default:
			if TestContext.OutputPrintType != "json" {
				Logf("Unknown output type: %v. Printing JSON", TestContext.OutputPrintType)
			}
			if TestContext.ReportDir == "" {
				Logf("%v JSON\n%v", summaries[i].SummaryKind(), summaries[i].PrintJSON())
				Logf("Finished")
			} else {
				// TODO: learn to extract test name and append it to the kind instead of timestamp.
				filePath := path.Join(TestContext.ReportDir, summaries[i].SummaryKind()+"_"+testBaseName+"_"+now.Format(time.RFC3339)+".json")
				Logf("Writing to %s", filePath)
				if err := ioutil.WriteFile(filePath, []byte(summaries[i].PrintJSON()), 0644); err != nil {
					Logf("Failed to write file %v with test performance data: %v", filePath, err)
				}
			}
		}
	}
}

func CreateTestingHostPathPV(baseName string, c clientset.Interface, isCSIPV bool, size int64, anns map[string]string) (*v1.PersistentVolume, error) {
	labels := map[string]string{}
	if anns == nil {
		anns = map[string]string{}
	}
	labels["e2e-run"] = string(RunId)

	pvs := v1.PersistentVolumeSource{
		HostPath: &v1.HostPathVolumeSource{
			Path: "/tmp/" + baseName,
		},
	}
	if isCSIPV {
		pvs = v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       "xfshostpathplugin",
				VolumeHandle: string(NewUUID()),
			},
		}
	}
	pvObj := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("e2e-tests-%v-", baseName),
			Labels:       labels,
			Annotations:  anns,
		},
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			Capacity:               v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(size, resource.DecimalSI)},
			PersistentVolumeSource: pvs,
		},
	}
	// Be robust about making the namespace creation call.
	var got *v1.PersistentVolume
	if err := wait.PollImmediate(Poll, 30*time.Second, func() (bool, error) {
		var err error
		got, err = c.CoreV1().PersistentVolumes().Create(pvObj)
		if err != nil {
			Logf("Unexpected error while creating PV: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	return got, nil
}

func CreateTestingHostPathPVC(baseName string, c clientset.Interface, size int64, pvcNS, volumeName string) (*v1.PersistentVolumeClaim, error) {
	labels := map[string]string{}
	labels["e2e-run"] = string(RunId)

	pvcObj := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("e2e-tests-%v-", baseName),
			Labels:       labels,
			Namespace:    pvcNS,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(size, resource.DecimalSI)},
			},
			VolumeName: volumeName,
		},
	}
	// Be robust about making the namespace creation call.
	var got *v1.PersistentVolumeClaim
	if err := wait.PollImmediate(Poll, 30*time.Second, func() (bool, error) {
		var err error
		got, err = c.CoreV1().PersistentVolumeClaims(pvcNS).Create(pvcObj)
		if err != nil {
			Logf("Unexpected error while creating PVC: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	return got, nil
}

func CreateTestingNS(baseName string, c clientset.Interface, labels map[string]string, anns map[string]string) (*v1.Namespace, error) {
	if labels == nil {
		labels = map[string]string{}
	}
	if anns == nil {
		anns = map[string]string{}
	}
	labels["e2e-run"] = string(RunId)

	namespaceObj := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("e2e-tests-%v-", baseName),
			Namespace:    "",
			Labels:       labels,
			Annotations:  anns,
		},
		Status: v1.NamespaceStatus{},
	}
	// Be robust about making the namespace creation call.
	var got *v1.Namespace
	if err := wait.PollImmediate(Poll, 30*time.Second, func() (bool, error) {
		var err error
		got, err = c.CoreV1().Namespaces().Create(namespaceObj)
		if err != nil {
			Logf("Unexpected error while creating namespace: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	return got, nil
}

func WaitForPodRunningInNamespace(c clientset.Interface, pod *v1.Pod) error {
	if pod.Status.Phase == v1.PodRunning {
		return nil
	}
	return WaitTimeoutForPodRunningInNamespace(c, pod.Name, pod.Namespace, PodStartTimeout)
}

// Waits default amount of time (PodStartTimeout) for the specified pod to become running.
// Returns an error if timeout occurs first, or pod goes in to failed state.
func WaitForPodNameRunningInNamespace(c clientset.Interface, podName, namespace string) error {
	return WaitTimeoutForPodRunningInNamespace(c, podName, namespace, PodStartTimeout)
}

// Waits an extended amount of time (slowPodStartTimeout) for the specified pod to become running.
// The resourceVersion is used when Watching object changes, it tells since when we care
// about changes to the pod. Returns an error if timeout occurs first, or pod goes in to failed state.
func waitForPodRunningInNamespaceSlow(c clientset.Interface, podName, namespace string) error {
	return WaitTimeoutForPodRunningInNamespace(c, podName, namespace, slowPodStartTimeout)
}

func WaitTimeoutForPodRunningInNamespace(c clientset.Interface, podName, namespace string, timeout time.Duration) error {
	return wait.PollImmediate(Poll, timeout, podRunning(c, podName, namespace))
}

func WaitForPodToDisappear(c clientset.Interface, ns, podName string, label labels.Selector, interval, timeout time.Duration) error {
	return wait.PollImmediate(interval, timeout, func() (bool, error) {
		Logf("Waiting for pod %s to disappear", podName)
		options := metav1.ListOptions{LabelSelector: label.String()}
		pods, err := c.CoreV1().Pods(ns).List(options)
		if err != nil {
			if testutils.IsRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		found := false
		for _, pod := range pods.Items {
			if pod.Name == podName {
				Logf("Pod %s still exists", podName)
				found = true
				break
			}
		}
		if !found {
			Logf("Pod %s no longer exists", podName)
			return true, nil
		}
		return false, nil
	})
}

// WaitForPodNameUnschedulableInNamespace returns an error if it takes too long for the pod to become Pending
// and have condition Status equal to Unschedulable,
// if the pod Get api returns an error (IsNotFound or other), or if the pod failed with an unexpected reason.
// Typically called to test that the passed-in pod is Pending and Unschedulable.
func WaitForPodNameUnschedulableInNamespace(c clientset.Interface, podName, namespace string) error {
	return WaitForPodCondition(c, namespace, podName, "Unschedulable", PodStartTimeout, func(pod *v1.Pod) (bool, error) {
		// Only consider Failed pods. Successful pods will be deleted and detected in
		// waitForPodCondition's Get call returning `IsNotFound`
		if pod.Status.Phase == v1.PodPending {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionFalse && cond.Reason == "Unschedulable" {
					return true, nil
				}
			}
		}
		if pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			return true, fmt.Errorf("Expected pod %q in namespace %q to be in phase Pending, but got phase: %v", podName, namespace, pod.Status.Phase)
		}
		return false, nil
	})
}

// WaitForService waits until the service appears (exist == true), or disappears (exist == false)
func WaitForService(c clientset.Interface, namespace, name string, exist bool, interval, timeout time.Duration) error {
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := c.CoreV1().Services(namespace).Get(name, metav1.GetOptions{})
		switch {
		case err == nil:
			Logf("Service %s in namespace %s found.", name, namespace)
			return exist, nil
		case apierrs.IsNotFound(err):
			Logf("Service %s in namespace %s disappeared.", name, namespace)
			return !exist, nil
		case !testutils.IsRetryableAPIError(err):
			Logf("Non-retryable failure while getting service.")
			return false, err
		default:
			Logf("Get service %s in namespace %s failed: %v", name, namespace, err)
			return false, nil
		}
	})
	if err != nil {
		stateMsg := map[bool]string{true: "to appear", false: "to disappear"}
		return fmt.Errorf("error waiting for service %s/%s %s: %v", namespace, name, stateMsg[exist], err)
	}
	return nil
}

// WaitForServiceWithSelector waits until any service with given selector appears (exist == true), or disappears (exist == false)
func WaitForServiceWithSelector(c clientset.Interface, namespace string, selector labels.Selector, exist bool, interval,
	timeout time.Duration) error {
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		services, err := c.CoreV1().Services(namespace).List(metav1.ListOptions{LabelSelector: selector.String()})
		switch {
		case len(services.Items) != 0:
			Logf("Service with %s in namespace %s found.", selector.String(), namespace)
			return exist, nil
		case len(services.Items) == 0:
			Logf("Service with %s in namespace %s disappeared.", selector.String(), namespace)
			return !exist, nil
		case !testutils.IsRetryableAPIError(err):
			Logf("Non-retryable failure while listing service.")
			return false, err
		default:
			Logf("List service with %s in namespace %s failed: %v", selector.String(), namespace, err)
			return false, nil
		}
	})
	if err != nil {
		stateMsg := map[bool]string{true: "to appear", false: "to disappear"}
		return fmt.Errorf("error waiting for service with %s in namespace %s %s: %v", selector.String(), namespace, stateMsg[exist], err)
	}
	return nil
}

//WaitForServiceEndpointsNum waits until the amount of endpoints that implement service to expectNum.
func WaitForServiceEndpointsNum(c clientset.Interface, namespace, serviceName string, expectNum int, interval, timeout time.Duration) error {
	return wait.Poll(interval, timeout, func() (bool, error) {
		Logf("Waiting for amount of service:%s endpoints to be %d", serviceName, expectNum)
		list, err := c.CoreV1().Endpoints(namespace).List(metav1.ListOptions{})
		if err != nil {
			return false, err
		}

		for _, e := range list.Items {
			if e.Name == serviceName && countEndpointsNum(&e) == expectNum {
				return true, nil
			}
		}
		return false, nil
	})
}

func WaitForPodCondition(c clientset.Interface, ns, podName, desc string, timeout time.Duration, condition podCondition) error {
	Logf("Waiting up to %v for pod %q in namespace %q to be %q", timeout, podName, ns, desc)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		pod, err := c.CoreV1().Pods(ns).Get(podName, metav1.GetOptions{})
		if err != nil {
			if apierrs.IsNotFound(err) {
				Logf("Pod %q in namespace %q not found. Error: %v", podName, ns, err)
				return err
			}
			Logf("Get pod %q in namespace %q failed, ignoring for %v. Error: %v", podName, ns, Poll, err)
			continue
		}
		// log now so that current pod info is reported before calling `condition()`
		Logf("Pod %q: Phase=%q, Reason=%q, readiness=%t. Elapsed: %v",
			podName, pod.Status.Phase, pod.Status.Reason, podutil.IsPodReady(pod), time.Since(start))
		if done, err := condition(pod); done {
			if err == nil {
				Logf("Pod %q satisfied condition %q", podName, desc)
			}
			return err
		}
	}
	return fmt.Errorf("Gave up after waiting %v for pod %q to be %q", timeout, podName, desc)
}

// WaitForMatchPodsCondition finds match pods based on the input ListOptions.
// waits and checks if all match pods are in the given podCondition
func WaitForMatchPodsCondition(c clientset.Interface, opts metav1.ListOptions, desc string, timeout time.Duration, condition podCondition) error {
	Logf("Waiting up to %v for matching pods' status to be %s", timeout, desc)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		pods, err := c.CoreV1().Pods(metav1.NamespaceAll).List(opts)
		if err != nil {
			return err
		}
		conditionNotMatch := []string{}
		for _, pod := range pods.Items {
			done, err := condition(&pod)
			if done && err != nil {
				return fmt.Errorf("Unexpected error: %v", err)
			}
			if !done {
				conditionNotMatch = append(conditionNotMatch, format.Pod(&pod))
			}
		}
		if len(conditionNotMatch) <= 0 {
			return err
		}
		Logf("%d pods are not %s: %v", len(conditionNotMatch), desc, conditionNotMatch)
	}
	return fmt.Errorf("gave up waiting for matching pods to be '%s' after %v", desc, timeout)
}

func WaitForPodScheduled(c clientset.Interface, podNs, podName string, timeout time.Duration) (string, error) {
	Logf("Waiting up to %v for pod %s/%s be scheduled", timeout, podNs, podName)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		pod, err := c.CoreV1().Pods(podNs).Get(podName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		if len(pod.Status.HostIP) > 0 {
			return pod.Status.HostIP, nil
		}
		Logf("pod %s/%s are not scheduled", podNs, podName)
	}
	return "", fmt.Errorf("gave up waiting for pod %s/%s scheduled after %v", podNs, podName, timeout)
}

func WaitForPodPhase(c clientset.Interface, podNs, podName string, phase v1.PodPhase, timeout time.Duration) (*v1.Pod, error) {
	Logf("Waiting up to %v for pod %s/%s be %s", timeout, podNs, podName, phase)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		pod, err := c.CoreV1().Pods(podNs).Get(podName, metav1.GetOptions{})
		if err != nil {
			return pod, err
		}
		if pod.Status.Phase == phase {
			return pod, nil
		}
		Logf("pod %s/%s phase is not equal %s", podNs, podName, phase)
	}
	return nil, fmt.Errorf("gave up waiting for pod %s/%s phase be %s after %v", podNs, podName, phase, timeout)
}

func DeleteAndWaitPodDelete(c clientset.Interface, podNs, podName string, timeout time.Duration) error {
	err := c.CoreV1().Pods(podNs).Delete(podName, nil)
	if err != nil {
		return err
	}
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		_, err := c.CoreV1().Pods(podNs).Get(podName, metav1.GetOptions{})
		if err != nil {
			if apierrs.IsNotFound(err) {
				return nil
			}
		}
	}
	return fmt.Errorf("gave up wait for pod %s/%s be deleted", podNs, podName)
}

func GetPodLogs(c clientset.Interface, namespace, podName, containerName string) (string, error) {
	return getPodLogsInternal(c, namespace, podName, containerName, false)
}

func getPreviousPodLogs(c clientset.Interface, namespace, podName, containerName string) (string, error) {
	return getPodLogsInternal(c, namespace, podName, containerName, true)
}

// utility function for gomega Eventually
func getPodLogsInternal(c clientset.Interface, namespace, podName, containerName string, previous bool) (string, error) {
	logs, err := c.CoreV1().RESTClient().Get().
		Resource("pods").
		Namespace(namespace).
		Name(podName).SubResource("log").
		Param("container", containerName).
		Param("previous", strconv.FormatBool(previous)).
		Do().
		Raw()
	if err != nil {
		return "", err
	}
	if err == nil && strings.Contains(string(logs), "Internal Error") {
		return "", fmt.Errorf("Fetched log contains \"Internal Error\": %q.", string(logs))
	}
	return string(logs), err
}

func podRunning(c clientset.Interface, podName, namespace string) wait.ConditionFunc {
	return func() (bool, error) {
		pod, err := c.CoreV1().Pods(namespace).Get(podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		switch pod.Status.Phase {
		case v1.PodRunning:
			return true, nil
		case v1.PodFailed, v1.PodSucceeded:
			return false, conditions.ErrPodCompleted
		}
		return false, nil
	}
}
func countEndpointsNum(e *v1.Endpoints) int {
	num := 0
	for _, sub := range e.Subsets {
		num += len(sub.Addresses)
	}
	return num
}

func ExpectNoError(err error, explain ...interface{}) {
	ExpectNoErrorWithOffset(1, err, explain...)
}

func ExpectNoErrorWithOffset(offset int, err error, explain ...interface{}) {
	if err != nil {
		Logf("Unexpected error occurred: %v", err)
	}
	ExpectWithOffset(1+offset, err).NotTo(HaveOccurred(), explain...)
}
