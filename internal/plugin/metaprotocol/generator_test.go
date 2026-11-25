// Copyright Aeraki Authors
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

package metaprotocol

import (
	"strings"
	"testing"

	userapi "github.com/aeraki-mesh/api/metaprotocol/v1alpha1"
	mpclient "github.com/aeraki-mesh/client-go/pkg/apis/metaprotocol/v1alpha1"
	networking "istio.io/api/networking/v1alpha3"
	istioconfig "istio.io/istio/pkg/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/aeraki-mesh/aeraki/internal/model"
)

func gatewayContextWithMetaRouter(metaRouter *mpclient.MetaRouter) *model.EnvoyFilterContext {
	return &model.EnvoyFilterContext{
		Gateway: &model.GatewayWrapper{
			Meta: istioconfig.Meta{Name: "test-gateway", Namespace: "meta-dubbo"},
			Spec: &networking.Gateway{},
		},
		ServiceEntry: &model.ServiceEntryWrapper{
			Spec: &networking.ServiceEntry{Hosts: []string{testHost}},
		},
		MetaRouter: metaRouter,
	}
}

func sidecarContextWithMetaRouter(metaRouter *mpclient.MetaRouter,
	serviceEntry *model.ServiceEntryWrapper) *model.EnvoyFilterContext {
	return &model.EnvoyFilterContext{
		ServiceEntry: serviceEntry,
		MetaRouter:   metaRouter,
	}
}

// TestRateLimitWarningsOnGateway ensures rate limit configuration on a gateway MetaRouter is
// exposed as warnings instead of being silently ignored, since gateways only get outbound proxies.
func TestRateLimitWarningsOnGateway(t *testing.T) {
	metaRouter := &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "gw-metarouter", Namespace: "meta-dubbo"},
		Spec: userapi.MetaRouter{
			Hosts:           []string{testHost},
			LocalRateLimit:  &userapi.LocalRateLimit{},
			GlobalRateLimit: &userapi.GlobalRateLimit{},
		},
	}

	warnings := rateLimitWarningsOnGateway(gatewayContextWithMetaRouter(metaRouter))
	if len(warnings) != 2 {
		t.Fatalf("warnings count = %d, want 2: %v", len(warnings), warnings)
	}
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "localRateLimit") || !strings.Contains(joined, "ignored") {
		t.Errorf("missing localRateLimit ignored warning: %v", warnings)
	}
	if !strings.Contains(joined, "globalRateLimit") {
		t.Errorf("missing globalRateLimit ignored warning: %v", warnings)
	}
}

func TestRateLimitWarningsOnGatewayWithoutMetaRouter(t *testing.T) {
	if warnings := rateLimitWarningsOnGateway(gatewayContextWithMetaRouter(nil)); warnings != nil {
		t.Errorf("expected no warnings without MetaRouter, got %v", warnings)
	}
}

func TestRateLimitWarningsOnGatewayWithoutRateLimit(t *testing.T) {
	metaRouter := &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "gw-metarouter", Namespace: "meta-dubbo"},
		Spec:       userapi.MetaRouter{Hosts: []string{testHost}},
	}
	if warnings := rateLimitWarningsOnGateway(gatewayContextWithMetaRouter(metaRouter)); warnings != nil {
		t.Errorf("expected no warnings without rate limit config, got %v", warnings)
	}
}

// TestRateLimitWarningsWithoutInboundTarget covers the sidecar case where the ServiceEntry selects
// no workload: the inbound EnvoyFilter (and therefore the rate limit filters) is never generated.
func TestRateLimitWarningsWithoutInboundTarget(t *testing.T) {
	metaRouter := &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "sidecar-metarouter", Namespace: "meta-dubbo"},
		Spec: userapi.MetaRouter{
			Hosts:          []string{testHost},
			LocalRateLimit: &userapi.LocalRateLimit{},
		},
	}

	// No workloadSelector and no annotation: a warning must be emitted.
	serviceEntry := &model.ServiceEntryWrapper{
		Meta: istioconfig.Meta{Name: "test-service-entry", Namespace: "meta-dubbo"},
		Spec: &networking.ServiceEntry{
			Hosts: []string{testHost},
			Ports: []*networking.ServicePort{{Name: "tcp-dubbo", Number: 20880}},
		},
	}
	warnings := rateLimitWarningsWithoutInboundTarget(sidecarContextWithMetaRouter(metaRouter, serviceEntry))
	if len(warnings) != 1 {
		t.Fatalf("warnings count = %d, want 1: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "localRateLimit") ||
		!strings.Contains(warnings[0], "no inbound EnvoyFilter is generated") {
		t.Errorf("warning does not explain the ineffective localRateLimit: %s", warnings[0])
	}
}

// TestNoWarningWhenWorkloadSelectorPresent verifies that a properly targeted ServiceEntry produces
// no ineffectiveness warning, so the warning does not cry wolf.
func TestNoWarningWhenWorkloadSelectorPresent(t *testing.T) {
	metaRouter := &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "sidecar-metarouter", Namespace: "meta-dubbo"},
		Spec: userapi.MetaRouter{
			Hosts:          []string{testHost},
			LocalRateLimit: &userapi.LocalRateLimit{},
		},
	}

	serviceEntry := &model.ServiceEntryWrapper{
		Spec: &networking.ServiceEntry{
			Hosts: []string{testHost},
			WorkloadSelector: &networking.WorkloadSelector{
				Labels: map[string]string{"app": "dubbo-sample-provider"},
			},
		},
	}
	if warnings := rateLimitWarningsWithoutInboundTarget(sidecarContextWithMetaRouter(metaRouter,
		serviceEntry)); warnings != nil {
		t.Errorf("expected no warnings with a workloadSelector, got %v", warnings)
	}
}

// TestNoWarningWhenWorkloadSelectorAnnotationPresent verifies the annotation-based selector is
// recognized the same way as an explicit ServiceEntry workloadSelector.
func TestNoWarningWhenWorkloadSelectorAnnotationPresent(t *testing.T) {
	metaRouter := &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "sidecar-metarouter", Namespace: "meta-dubbo"},
		Spec: userapi.MetaRouter{
			Hosts:          []string{testHost},
			LocalRateLimit: &userapi.LocalRateLimit{},
		},
	}

	serviceEntry := &model.ServiceEntryWrapper{
		Meta: istioconfig.Meta{
			Annotations: map[string]string{"workloadSelector": "app: dubbo-sample-provider"},
		},
		Spec: &networking.ServiceEntry{Hosts: []string{testHost}},
	}
	if warnings := rateLimitWarningsWithoutInboundTarget(sidecarContextWithMetaRouter(metaRouter,
		serviceEntry)); warnings != nil {
		t.Errorf("expected no warnings with a workloadSelector annotation, got %v", warnings)
	}
}

func TestNoWarningWithoutRateLimit(t *testing.T) {
	metaRouter := &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "sidecar-metarouter", Namespace: "meta-dubbo"},
		Spec:       userapi.MetaRouter{Hosts: []string{testHost}},
	}
	serviceEntry := &model.ServiceEntryWrapper{
		Spec: &networking.ServiceEntry{Hosts: []string{testHost}},
	}
	if warnings := rateLimitWarningsWithoutInboundTarget(sidecarContextWithMetaRouter(metaRouter,
		serviceEntry)); warnings != nil {
		t.Errorf("expected no warnings without rate limit config, got %v", warnings)
	}
}
