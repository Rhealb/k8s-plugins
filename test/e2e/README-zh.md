# K8splugin e2e test
　
## 说明

[English](README.md) | [中文](README-zh.md)

**K8splugin 是我们在k8s上通过plugin的方式加的我们一些特有的功能，而本模块主要是端到端的测试各个模块的可用性和完整性．本测试框架是参考k8s的e2e测试框架基于ginkgo+gomega．**

## 测试
+ **1)下载代码：**

        $ git clone ssh://git@gitlab.cloud.enndata.cn:10885/kubernetes/k8s-plugins.git
        $ cd k8s-plugins/test/e2e
	  
+ **2)正常测试：**

        $ make test

+ **3)debug测试：**

	    $ make debug
        （与make test的区别是测试过程中创建的namespace, pv, pod等不会被删除以便调试，可以结合下面的FOCUS和SKIP使用）

+ **4)FOCUS测试：**

	    $ make test FOCUS=EnndataScheduler
	    （只会测试EnndataScheduler模块，其他模块会被SKIP,如果是debug测试可以make debug FOCUS=EnndataScheduler）

+ **5)SKIP测试：**

	    $ make test SKIP=EnndataScheduler
        (EnndataScheduler模块会被跳过其他模块会被测试，可以用于某些模块没有部署所以可以跳过不测．)

+ **6)编译测试：**

	    $ make build
	    $ ./e2e.test -kubeconfig=/home/adam/.kube/config -cluster-register=10.19.140.200:29006
	    (之前的所有测试其实也是先编译然后再测试的，但是如果没有代码或者golang编译环境的地方可以将编译之后的e2e.test可执行文件拷贝到测试环境上测试功能和之前一样，测试参数可以参考makefile)
	    
+ **5)测试清理：**

	    $ make clean	    
        (如果之前有用过make debug测试生成的namespace, pv，pod等资源会被保留，可以用make clean进行清理)
        
 + **6)加载测试image：**

	    $ make image REGISTRY=10.19.140.200:29006  
        (如果环境是第一次测试，需要准备测试pod所需image, 如上所需image将被push到registry 10.19.140.200:29006.
       注意：目前的测试只是用到了apline这个image,如果新的测试需要新的image需要修改makefile里的image,详情可以查看makefile)   
        
        
## 主要测试参数：    
+ **1) host:** 连apiserver的地址，当--kubeconfig没有设置默认为：http://127.0.0.1:8080．

+ **2) kubeconfig:** 连apiserver的配置文件．

+ **3) delete-namespace:** 测试过程中生成的namespace是否删除，默认为true.

+ **4) delete-namespace-on-failure:** 测试过程中如果出错，生成的namespace是否删除，默认为true（一般在调试的时候使用）.

+ **5) delete-pv:** 测试过程中生成的PV是否删除，默认为true.

+ **6) delete-pv-on-failure:** 测试过程中如果出错，生成的PV是否删除，默认为true（一般在调试的时候使用）.

+ **7) cluster-register:** 测试过程用所生成的Pod Image所用的register地址，默认为：127.0.0.1:29006.

+ **8) context:** 覆盖kubeconfig里的current-context.

+ **9) kube-api-content-type:** 和apiserver通信用的数据格式默认：application/vnd.kubernetes.protobuf.

+ **10) cluster-register:** 测试过程用所生成的Pod Image所用的register地址，默认为：127.0.0.1:29006.