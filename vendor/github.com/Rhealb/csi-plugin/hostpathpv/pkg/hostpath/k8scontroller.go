package hostpath

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	glog "k8s.io/klog"
)

const (
	minSupportMajor     int = 1
	minSupportMinor     int = 9
	minSupportSubMinor  int = 2
	minSupportCommitNum int = 35
)

type k8sInterface interface {
	IsPodExist(podId string) bool
	IsPodExistCheckCacheOnly(podId string) bool
	//SetPodExist(podId string, exist bool)
	GetParentId(podId string) string
	GetCSIPVByVolumeId(volumeId string) *v1.PersistentVolume
	ListCSIPV(driver string) ([]*v1.PersistentVolume, error)
	ListHostPathPV() ([]*v1.PersistentVolume, error)
	UpdatePV(pv *v1.PersistentVolume) error
	GetPodNsAndNameByUID(uid string) (ns, name string)
	GetNodeByName(name string) (*v1.Node, error)
	GetPVByName(name string) (*v1.PersistentVolume, error)
	UpdateNode(node *v1.Node) error
	IsNodeSupport(nodeName string) (ok bool, curVer, minVer string)
	GetPodByPodId(id string) (*v1.Pod, error)
	GetPVCByName(ns, name string) (*v1.PersistentVolumeClaim, error)
}
type k8scontroller struct {
	client           kubernetes.Interface
	pvLister         corelisters.PersistentVolumeLister
	podLister        corelisters.PodLister
	podParentIdCache *fifoCache
	podExistCache    *fifoCache
	podNameCache     *fifoCache
}

func (k8s *k8scontroller) Start(informerResync time.Duration) error {
	factory := informers.NewSharedInformerFactory(k8s.client, informerResync)
	pvInformer := factory.Core().V1().PersistentVolumes()
	podInformer := factory.Core().V1().Pods()
	pvListerSynced := pvInformer.Informer().HasSynced
	podListerSynced := podInformer.Informer().HasSynced
	k8s.pvLister = pvInformer.Lister()
	k8s.podLister = podInformer.Lister()
	if k8s.podParentIdCache == nil {
		k8s.podParentIdCache = newCache(100)
	}
	if k8s.podExistCache == nil {
		k8s.podExistCache = newCache(100)
	}
	if k8s.podNameCache == nil {
		k8s.podNameCache = newCache(100)
	}
	stopCh := make(chan struct{})
	factory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, pvListerSynced, podListerSynced) {
		return fmt.Errorf("Cannot sync caches")
	}
	return nil
}

func (k8s *k8scontroller) IsPodExistCheckCacheOnly(podId string) bool {
	pods, _ := k8s.podLister.List(labels.Everything())
	for _, pod := range pods {
		if string(pod.UID) == podId {
			return true
		}
	}
	return false
}

func (k8s *k8scontroller) IsPodExist(podId string) bool {
	if podId == "" {
		return false
	}
	if value, ok := k8s.podExistCache.getValue(podId).(bool); ok == true && value == false { // pod is not exist
		return false
	}
	pods, _ := k8s.podLister.List(labels.Everything())
	for _, pod := range pods {
		if string(pod.UID) == podId {
			if ok := isPodOk(pod); ok == false {
				k8s.podExistCache.setValue(podId, false)
				return false
			} else {
				curPod, err := k8s.getPod(pod.Namespace, pod.Name)
				if err == nil && string(curPod.UID) == podId {
					isOk := isPodOk(curPod)
					k8s.podExistCache.setValue(string(curPod.UID), isOk)
					return isOk
				} else if apierrors.IsNotFound(err) {
					k8s.podExistCache.setValue(podId, false)
					return false
				}
			}
		}
	}
	return false
}

func (k8s *k8scontroller) getPod(ns, name string) (*v1.Pod, error) {
	return k8s.client.CoreV1().Pods(ns).Get(name, meta_v1.GetOptions{})
}

func (k8s *k8scontroller) SetPodExist(podId string, exist bool) {
	if podId == "" {
		return
	}
	k8s.podExistCache.setValue(podId, exist)
}

func (k8s *k8scontroller) GetParentId(podId string) string {
	if value, ok := k8s.podParentIdCache.getValue(podId).(string); ok == true {
		return value
	}

	pods, _ := k8s.podLister.List(labels.Everything())
	for _, pod := range pods {
		if string(pod.UID) == podId {
			parentId := ""
			if len(pod.OwnerReferences) > 0 {
				parentId = string(pod.OwnerReferences[0].UID)
			}
			k8s.podParentIdCache.setValue(podId, parentId)
			return parentId
		}
	}

	return ""
}

func (k8s *k8scontroller) GetPodNsAndNameByUID(uid string) (ns, name string) {
	if value, ok := k8s.podNameCache.getValue(uid).(string); ok == true {
		strs := strings.Split(value, ":")
		if len(strs) >= 2 {
			return strs[0], strs[1]
		}
		return "", ""
	}

	pods, _ := k8s.podLister.List(labels.Everything())
	for _, pod := range pods {
		if string(pod.UID) == uid {
			k8s.podNameCache.setValue(uid, fmt.Sprintf("%s:%s", pod.Namespace, pod.Name))
			return pod.Namespace, pod.Name
		}
	}
	return "", ""
}

func (k8s *k8scontroller) GetPVByName(name string) (*v1.PersistentVolume, error) {
	return k8s.client.CoreV1().PersistentVolumes().Get(name, meta_v1.GetOptions{})
}

func (k8s *k8scontroller) ListCSIPV(driver string) ([]*v1.PersistentVolume, error) {
	pvs, err := k8s.pvLister.List(labels.Everything())
	if err != nil {
		return []*v1.PersistentVolume{}, err
	}
	ret := make([]*v1.PersistentVolume, 0, len(pvs))
	for _, pv := range pvs {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == driver {
			ret = append(ret, pv)
		}
	}
	return ret, nil
}

func (k8s *k8scontroller) ListHostPathPV() ([]*v1.PersistentVolume, error) {
	pvs, err := k8s.pvLister.List(labels.Everything())
	if err != nil {
		return []*v1.PersistentVolume{}, err
	}
	ret := make([]*v1.PersistentVolume, 0, len(pvs))
	for _, pv := range pvs {
		if pv.Spec.HostPath != nil {
			ret = append(ret, pv)
		}
	}
	return ret, nil
}

func (k8s *k8scontroller) GetCSIPVByVolumeId(volumeId string) *v1.PersistentVolume {
	pvs, _ := k8s.pvLister.List(labels.Everything())
	for _, pv := range pvs {
		if pv.Spec.CSI != nil {
			if pv.Spec.CSI.VolumeHandle == volumeId {
				return pv
			}
		}
	}
	return nil
}

func (k8s *k8scontroller) GetPodByPodId(id string) (*v1.Pod, error) {
	pods, _ := k8s.podLister.List(labels.Everything())
	for _, pod := range pods {
		if string(pod.UID) == id {
			return pod, nil
		}
	}
	return nil, fmt.Errorf("podid %s is not found", id)
}

func (k8s *k8scontroller) GetPVCByName(ns, name string) (*v1.PersistentVolumeClaim, error) {
	return k8s.client.CoreV1().PersistentVolumeClaims(ns).Get(name, meta_v1.GetOptions{})
}

func (k8s *k8scontroller) GetNodeByName(name string) (*v1.Node, error) {
	return k8s.client.CoreV1().Nodes().Get(name, meta_v1.GetOptions{})
}

func (k8s *k8scontroller) IsNodeSupport(nodeName string) (ok bool, curVer, minVer string) {
	minVer = fmt.Sprintf("v%d.%d.%d-%d", minSupportMajor, minSupportMinor, minSupportSubMinor, minSupportCommitNum)
	node, err := k8s.GetNodeByName(nodeName)
	if err != nil {
		glog.Errorf("IsNodeSupport return false by getNode err:%v", err)
		return false, "", minVer
	}
	major, minor, subMinor, commitNum, _, errVer := parseVersion(node.Status.NodeInfo.KubeletVersion)
	if errVer != nil {
		glog.Errorf("IsNodeSupport return false by parseVersion err:%v", errVer)
		return false, node.Status.NodeInfo.KubeletVersion, minVer
	}
	if major > minSupportMajor {
		return true, node.Status.NodeInfo.KubeletVersion, minVer
	} else if major == minSupportMajor && minor > minSupportMinor {
		return true, node.Status.NodeInfo.KubeletVersion, minVer
	} else if major == minSupportMajor && minor == minSupportMinor && subMinor > minSupportSubMinor {
		return true, node.Status.NodeInfo.KubeletVersion, minVer
	} else if major == minSupportMajor && minor == minSupportMinor && subMinor == minSupportSubMinor && commitNum >= minSupportCommitNum {
		return true, node.Status.NodeInfo.KubeletVersion, minVer
	}
	return false, node.Status.NodeInfo.KubeletVersion, minVer
}

func (k8s *k8scontroller) UpdateNode(node *v1.Node) error {
	_, err := k8s.client.CoreV1().Nodes().Update(node)
	return err
}

func (k8s *k8scontroller) UpdatePV(pv *v1.PersistentVolume) error {
	_, err := k8s.client.CoreV1().PersistentVolumes().Update(pv)
	return err
}

func isPodOk(pod *v1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded {
		return false
	}
	return true
}

func parseVersion(ver string) (major, minor, subMinor, commitNum int, commitHash string, err error) {
	if strings.HasPrefix(ver, "v") == false { // invalid version string
		return 0, 0, 0, 0, "", fmt.Errorf("%s invalid version string", ver)
	}
	ver = ver[1:]
	index := strings.Index(ver, ".")
	if index > 0 {
		major, err = strconv.Atoi(ver[:index])
		if err != nil {
			return
		}
		ver = ver[index+1:]
	}
	index = strings.Index(ver, ".")
	if index > 0 {
		minor, err = strconv.Atoi(ver[:index])
		if err != nil {
			return
		}
		ver = ver[index+1:]
	}
	index = strings.Index(ver, "-")
	if index > 0 {
		subMinor, err = strconv.Atoi(ver[:index])
		if err != nil {
			return
		}
		ver = ver[index+1:]
	}

	index = strings.Index(ver, "+")
	if index > 0 {
		commitNum, err = strconv.Atoi(ver[:index])
		if err != nil {
			return
		}
		ver = ver[index+1:]
	}
	commitHash = ver
	return
}

type cacheItem struct {
	key   string
	value interface{}
}

type fifoCache struct {
	queue    []*cacheItem
	curIndex int
	mu       sync.Mutex
	cacheMap map[string]int
}

func newCache(size int) *fifoCache {
	return &fifoCache{
		queue:    make([]*cacheItem, size),
		cacheMap: make(map[string]int),
		curIndex: -1,
	}
}

func (c *fifoCache) isKeyExist(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exist := c.cacheMap[key]
	return exist
}

func (c *fifoCache) getValue(key string) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	index, exist := c.cacheMap[key]
	if exist == false || index < 0 || index >= len(c.queue) {
		return nil
	}
	return c.queue[index].value
}

func (c *fifoCache) setValue(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	index, exist := c.cacheMap[key]
	if exist == true && index >= 0 && index < len(c.queue) { // exist update value
		c.queue[index].value = value
		return
	}
	// not exist add
	c.curIndex++
	c.curIndex = c.curIndex % len(c.queue)
	if c.queue[c.curIndex] != nil {
		oldValue := c.queue[c.curIndex]
		delete(c.cacheMap, oldValue.key)
	}
	c.queue[c.curIndex] = &cacheItem{
		key:   key,
		value: value,
	}
	c.cacheMap[key] = c.curIndex
}
