package enndatascheduler

import "k8s-plugins/test/e2e/framework"

func SIGDescribe(text string, body func()) bool {
	return framework.EnndataDescribe("[sig-enndatascheduler] "+text, body)
}
