REGISTRY?=ihub.helium.io:29006
FOCUS?=
SKIP?=

build:
	ginkgo build
	
image:
	docker load -i images/alpine.tar
	docker tag ihub.helium.io:29006/library/apline:latest `echo "ihub.helium.io:29006/library/apline:latest" | sed 's/ihub.helium.io:29006/${REGISTRY}/g'`
	docker push `echo "ihub.helium.io:29006/library/apline:latest" | sed 's/ihub.helium.io:29006/${REGISTRY}/g'`
	
clean:
	@rm e2e.test 1>/dev/null 2>/dev/null || true
	@kubectl delete namespace `kubectl get ns | grep e2e | awk '{print $$1}'` 2>/dev/null 1>/dev/null || true
	@kubectl delete pv `kubectl get pv | grep e2e | awk '{print $$1}'`  2>/dev/null 1>/dev/null || true
	
test:
	ginkgo -- -kubeconfig=/home/adam/.kube/config -report-prefix=enndata -report-dir=./report -cluster-register=${REGISTRY} -ginkgo.slowSpecThreshold=100 -ginkgo.focus=${FOCUS} -ginkgo.skip=${SKIP}
	
debug:
	ginkgo -- -kubeconfig=/home/adam/.kube/config -report-prefix=enndata -report-dir=./report -cluster-register=${REGISTRY} -ginkgo.slowSpecThreshold=100 ${FOCUS} -ginkgo.skip=${SKIP} -delete-namespace=false -delete-pv=false