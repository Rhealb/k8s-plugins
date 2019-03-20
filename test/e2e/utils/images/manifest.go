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

package image

import (
	"fmt"
	"runtime"

	"github.com/k8s-plugins/test/e2e/framework"
)

type ImageConfig struct {
	registry string
	name     string
	version  string
	hasArch  bool
}

func (i *ImageConfig) SetRegistry(registry string) {
	i.registry = registry
}

func (i *ImageConfig) SetName(name string) {
	i.name = name
}

func (i *ImageConfig) SetVersion(version string) {
	i.version = version
}

var (
	Alpine = ImageConfig{framework.TestContext.ClusterRegistry, "library/alpine", "latest", false}
	Pause  = ImageConfig{framework.TestContext.ClusterRegistry, "library/pause", "3.0", false}
)

func GetE2EImage(image ImageConfig) string {
	return GetE2EImageWithArch(image, runtime.GOARCH)
}

func GetE2EImageWithArch(image ImageConfig, arch string) string {
	registry := image.registry
	if framework.TestContext.ClusterRegistry != "" {
		registry = framework.TestContext.ClusterRegistry
	}
	if image.hasArch {
		return fmt.Sprintf("%s/%s-%s:%s", registry, image.name, arch, image.version)
	} else {
		return fmt.Sprintf("%s/%s:%s", registry, image.name, image.version)
	}
}

// GetPauseImageName returns the pause image name with proper version
func GetPauseImageName() string {
	return GetE2EImage(Pause)
}

func GetAlpineImageName() string {
	return GetE2EImage(Alpine)
}
