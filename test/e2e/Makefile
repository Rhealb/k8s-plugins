REGISTRY?=ihub.helium.io:29006
FOCUS?=
SKIP?=

ROOTPATH=$(shell pwd)
BUILDGOPATH=/tmp/k8splugin-build
BUILDPATH=$(BUILDGOPATH)/src/github.com/k8s-plugins/test

GOPATH=$(ROOTPATH)/staging/:$(BUILDGOPATH)

buildEnvClean:
	@rm -rf $(BUILDGOPATH) 1>/dev/null 2>/dev/null || true

buildEnv: buildEnvClean	
	@mkdir -p $(BUILDGOPATH)/src/github.com/k8s-plugins/
	@mkdir -p $(BUILDGOPATH)/src/github.com/Rhealb/csi-plugin/hostpathpv/
	@ln -s $(ROOTPATH)/../../../k8s-plugins/csi-plugin/hostpathpv/pkg $(BUILDGOPATH)/src/github.com/Rhealb/csi-plugin/hostpathpv/pkg
	@ln -s $(ROOTPATH)/../../test $(BUILDPATH)
	
build:buildEnv
	GOPATH=$(GOPATH) ginkgo build
	@rm -rf staging/pkg/ 1>/dev/null 2>/dev/null || true
	
image:
	docker load -i images/alpine.tar
	docker tag ihub.helium.io:29006/library/apline:latest `echo "ihub.helium.io:29006/library/apline:latest" | sed 's/ihub.helium.io:29006/${REGISTRY}/g'`
	docker push `echo "ihub.helium.io:29006/library/apline:latest" | sed 's/ihub.helium.io:29006/${REGISTRY}/g'`
	
clean:
	@rm e2e.test 1>/dev/null 2>/dev/null || true
	@rm -rf staging/pkg/ 1>/dev/null 2>/dev/null || true
	@kubectl delete namespace `kubectl get ns | grep e2e | awk '{print $$1}'` 2>/dev/null 1>/dev/null || true
	@kubectl delete pv `kubectl get pv | grep e2e | awk '{print $$1}'`  2>/dev/null 1>/dev/null || true
	
test: buildEnv
	GOPATH=$(GOPATH) ginkgo -- -kubeconfig=$(HOME)/.kube/config -report-prefix=enndata -report-dir=/tmp/report -cluster-register=${REGISTRY} -ginkgo.slowSpecThreshold=100 -ginkgo.focus=${FOCUS} -ginkgo.skip=${SKIP}
	@rm -rf staging/pkg/ 1>/dev/null 2>/dev/null || true
	
debug: buildEnv
	GOPATH=$(GOPATH) ginkgo -- -kubeconfig=$(HOME)/.kube/config -report-prefix=enndata -report-dir=/tmp/report -cluster-register=${REGISTRY} -ginkgo.slowSpecThreshold=100 -ginkgo.focus=${FOCUS} -ginkgo.skip=${SKIP} -delete-namespace=false -delete-pv=false
	@rm -rf staging/pkg/ 1>/dev/null 2>/dev/null || true