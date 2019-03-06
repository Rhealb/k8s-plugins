/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package predicates

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

const (
	nsnodeselector_configmap_ns   = "kube-system"
	nsnodeselector_configmap_name = "nsnodeselector"
	Nsnodeselector_systemlabel    = "enndata.cn/systemnode"
	nsnodeselector_itemname       = "nsnodeselector.json"
)

var (
	ErrNsNodeSelectorNotMatch = newPredicateFailureError("NamespaceNodeSelector", "NamespaceNodeSelector")
)

var nsNodeSelectorConfigMap *v1.ConfigMap = nil
var nsNodeSelectorConfigMapUpdateTime time.Time
var nsNodeSelectorConfigMapCacheTimeOut time.Duration = 5 * time.Second

//SetNsNodeSelectorConfigMapNil is used for test
func SetNsNodeSelectorConfigMapNil() {
	nsNodeSelectorConfigMap = nil
	nsNodeSelectorConfigMapUpdateTime = time.Date(2017, 1, 1, 1, 1, 1, 1, time.UTC)
}

func GetNsNodeSelectorConfigMap(client clientset.Interface) *v1.ConfigMap {
	if nsNodeSelectorConfigMap != nil && time.Since(nsNodeSelectorConfigMapUpdateTime) <= nsNodeSelectorConfigMapCacheTimeOut {
		return nsNodeSelectorConfigMap
	}
	cm, err := client.CoreV1().ConfigMaps(nsnodeselector_configmap_ns).Get(nsnodeselector_configmap_name, meta_v1.GetOptions{})
	if err != nil || cm == nil {
		//glog.Errorf("GetNsNodeSelectorConfigMap %s:%s , err:%v", nsnodeselector_configmap_ns, nsnodeselector_configmap_name, err)

		return &v1.ConfigMap{
			ObjectMeta: meta_v1.ObjectMeta{
				Namespace: nsnodeselector_configmap_ns,
				Name:      nsnodeselector_configmap_name,
			},
			Data: map[string]string{},
		}
	}
	nsNodeSelectorConfigMap = cm
	nsNodeSelectorConfigMapUpdateTime = time.Now()
	return cm
}

func SetNsNodeSelectorConfigMapNsConfig(cm *v1.ConfigMap, nsc NsConfig) error {
	if cm == nil {
		return fmt.Errorf("cm == nil")
	}
	buf, _ := json.MarshalIndent(nsc, " ", "  ")
	cm.Data[nsnodeselector_itemname] = string(buf)
	return nil
}

type LabelValues map[string]map[string]struct{}

func (lvs LabelValues) InsertValue(k, v string) {
	var strs []string
	if v != "" {
		strs = strings.Split(v, ",")
	} else {
		strs = []string{""}
	}
	if valueMap, exist := lvs[k]; exist == true {
		for _, str := range strs {
			valueMap[str] = struct{}{}
		}
	} else {
		valueMap = make(map[string]struct{})
		for _, str := range strs {
			valueMap[str] = struct{}{}
		}
		lvs[k] = valueMap
	}
}

type NsConfigItem struct {
	Match        LabelValues // it's used by new created pod, created pod if not match will not be deleted
	MustMatch    LabelValues // it's used by new created pod, created pod if not match will deleted by controller
	NotMatch     LabelValues // it's used by new created pod, created pod if match will not be deleted
	MustNotMatch LabelValues // it's used by new created pod, created pod if match will deleted by controller
}

type NsConfig map[string]NsConfigItem

type NsNodeSelector struct {
	nsConfig NsConfig
	cmGeter  func() *v1.ConfigMap
}

func GetNsConfigByConfigMap(cm *v1.ConfigMap, namespaces []*v1.Namespace) NsConfig {
	ret := make(map[string]NsConfigItem)
	var data string = "{}"

	if cm != nil && cm.Data[nsnodeselector_itemname] != "" {
		data = cm.Data[nsnodeselector_itemname]
	}

	err := json.Unmarshal([]byte(data), &ret)
	if err != nil {
		glog.Errorf("getNsConfigByConfigMap Unmarshal error:%v\n", err)
		return nil
	}
	if namespaces != nil {
		for _, ns := range namespaces {
			if _, find := ret[ns.Name]; find == false {
				ret[ns.Name] = NsConfigItem{}
			}
		}
	}
	for ns, item := range ret {
		if item.NotMatch == nil { // default namespace can't schedule to systemlabel node
			item.NotMatch = make(LabelValues)
			item.NotMatch[Nsnodeselector_systemlabel] = map[string]struct{}{"*": struct{}{}}
			//item.NotMatch.Insert(MakeSelectorByKeyValue(Nsnodeselector_systemlabel, "*"))
		}
		ret[ns] = item
	}
	return ret
}

func NewNamespaceNodeSelectorPredicate(cmGet func() *v1.ConfigMap) algorithm.FitPredicate {
	nsNodeSelector := &NsNodeSelector{
		nsConfig: GetNsConfigByConfigMap(cmGet(), nil),
		cmGeter:  cmGet,
	}
	return nsNodeSelector.CheckNamespaceNodeSelector
}

func MapHasString(m map[string]struct{}, strs ...string) bool {
	for _, str := range strs {
		if _, ok := m[str]; ok == true {
			return true
		}
	}
	return false
}

func ListMapString(m map[string]struct{}) []string {
	ret := make([]string, 0, len(m))
	for str := range m {
		ret = append(ret, str)
	}
	return ret
}

func GetNsLabelSelector(nsConfig NsConfig, ns string) (labels.Selector, error) {
	if nsConfig == nil {
		ret, err := labels.Parse(fmt.Sprintf("!%s", Nsnodeselector_systemlabel))
		if err != nil {
			return labels.Everything(), nil
		}
		return ret, nil
	}
	if nsConfigItem, exist := nsConfig[ns]; exist == false {
		ret, err := labels.Parse(fmt.Sprintf("!%s", Nsnodeselector_systemlabel))
		if err != nil {
			return labels.Everything(), nil
		}
		return ret, nil
	} else {
		var countItem int
		if nsConfigItem.Match == nil {
			nsConfigItem.Match = make(LabelValues)
		}
		if nsConfigItem.MustMatch == nil {
			nsConfigItem.MustMatch = make(LabelValues)
		}
		if nsConfigItem.NotMatch == nil {
			nsConfigItem.NotMatch = make(LabelValues)
		}
		if nsConfigItem.MustNotMatch == nil {
			nsConfigItem.MustNotMatch = make(LabelValues)
		}
		countItem = len(nsConfigItem.Match) + len(nsConfigItem.MustMatch) + len(nsConfigItem.NotMatch) + len(nsConfigItem.MustNotMatch)
		strTmps := make([]string, 0, countItem)

		for key, mapValue := range nsConfigItem.Match {
			if key != "" {
				if MapHasString(mapValue, "", "*") == false {
					strTmps = append(strTmps, fmt.Sprintf("%s in (%s)", key, strings.Join(ListMapString(mapValue), ",")))
				} else {
					strTmps = append(strTmps, fmt.Sprintf("%s", key))
				}
			}
		}
		for key, mapValue := range nsConfigItem.MustMatch {
			if key != "" {
				if MapHasString(mapValue, "", "*") == false {
					strTmps = append(strTmps, fmt.Sprintf("%s in (%s)", key, strings.Join(ListMapString(mapValue), ",")))
				} else {
					strTmps = append(strTmps, fmt.Sprintf("%s", key))
				}
			}
		}

		for key, mapValue := range nsConfigItem.NotMatch {
			if key != "" {
				if MapHasString(mapValue, "", "*") == false {
					strTmps = append(strTmps, fmt.Sprintf("%s notin (%s)", key, strings.Join(ListMapString(mapValue), ",")))
				} else {
					strTmps = append(strTmps, fmt.Sprintf("!%s", key))
				}
			}
		}
		for key, mapValue := range nsConfigItem.MustNotMatch {
			if key != "" {
				if MapHasString(mapValue, "", "*") == false {
					strTmps = append(strTmps, fmt.Sprintf("%s notin (%s)", key, strings.Join(ListMapString(mapValue), ",")))
				} else {
					strTmps = append(strTmps, fmt.Sprintf("!%s", key))
				}
			}
		}
		return labels.Parse(strings.Join(strTmps, ","))

	}
	return labels.Everything(), nil
}

func (nsns *NsNodeSelector) CheckNamespaceNodeSelector(pod *v1.Pod, meta algorithm.PredicateMetadata, nodeInfo *schedulercache.NodeInfo) (bool, []algorithm.PredicateFailureReason, error) {
	nsns.nsConfig = GetNsConfigByConfigMap(nsns.cmGeter(), nil)
	if nsns.nsConfig == nil {
		return true, nil, nil
	}

	node := nodeInfo.Node()
	if node == nil {
		return false, nil, fmt.Errorf("node not found")
	}

	selector, err := GetNsLabelSelector(nsns.nsConfig, pod.Namespace)
	if err != nil {
		return false, nil, err
	}
	if !selector.Matches(labels.Set(node.Labels)) {
		return false, []algorithm.PredicateFailureReason{ErrNsNodeSelectorNotMatch}, nil
	}
	return true, nil, nil
}
