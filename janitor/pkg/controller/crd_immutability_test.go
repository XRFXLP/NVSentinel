// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	janitorv1alpha1 "github.com/nvidia/nvsentinel/janitor/api/v1alpha1"
)

var _ = Describe("Maintenance CRD immutability", func() {
	It("prevents changing RebootNode nodeName", func() {
		resource := &janitorv1alpha1.RebootNode{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("immutable-reboot-%d", time.Now().UnixNano())},
			Spec:       janitorv1alpha1.RebootNodeSpec{NodeName: "node-a"},
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		resource.Spec.NodeName = "node-b"
		Expect(k8sClient.Update(ctx, resource)).To(MatchError(ContainSubstring(
			"nodeName cannot be changed after creation",
		)))
	})

	It("prevents changing TerminateNode nodeName", func() {
		resource := &janitorv1alpha1.TerminateNode{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("immutable-terminate-%d", time.Now().UnixNano())},
			Spec:       janitorv1alpha1.TerminateNodeSpec{NodeName: "node-a"},
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		resource.Spec.NodeName = "node-b"
		Expect(k8sClient.Update(ctx, resource)).To(MatchError(ContainSubstring(
			"nodeName cannot be changed after creation",
		)))
	})

	It("prevents changing the GPUReset selector", func() {
		resource := &janitorv1alpha1.GPUReset{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("immutable-gpu-reset-%d", time.Now().UnixNano())},
			Spec: janitorv1alpha1.GPUResetSpec{
				NodeName: "node-a",
				Selector: &janitorv1alpha1.GPUSelector{
					UUIDs: []string{"GPU-11111111-1111-1111-1111-111111111111"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		resource.Spec.Selector.UUIDs = []string{"GPU-22222222-2222-2222-2222-222222222222"}
		Expect(k8sClient.Update(ctx, resource)).To(MatchError(ContainSubstring(
			"selector cannot be changed after creation",
		)))
	})
})
