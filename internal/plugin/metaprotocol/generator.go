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
	"fmt"

	istionetworking "istio.io/api/networking/v1alpha3"
	"istio.io/pkg/log"

	"github.com/aeraki-mesh/aeraki/internal/envoyfilter"
	"github.com/aeraki-mesh/aeraki/internal/model"
	"github.com/aeraki-mesh/aeraki/internal/model/protocol"
)

var generatorLog = log.RegisterScope("metaprotocol-generator", "metaprotocol generator", 0)

// Generator defines a MetaProtocol envoyfilter Generator
type Generator struct {
}

// NewGenerator creates an new MetaProtocol Generator instance
func NewGenerator() *Generator {
	return &Generator{}
}

// Generate create EnvoyFilters for MetaProtocol services
func (*Generator) Generate(context *model.EnvoyFilterContext) ([]*model.EnvoyFilterWrapper, error) {
	if context.Gateway != nil {
		return generateGatewayEnvoyFilters(context)
	}
	return generateSidecarEnvoyFilters(context)
}

func generateGatewayEnvoyFilters(context *model.EnvoyFilterContext) ([]*model.EnvoyFilterWrapper, error) {
	var envoyfilters []*model.EnvoyFilterWrapper
	// The local and global rate limit filters are only appended on the inbound filter chain (see
	// buildInboundFilters), while a gateway only gets an outbound proxy. Expose the ignored
	// configuration with a warning instead of silently generating an EnvoyFilter without it.
	for _, warning := range rateLimitWarningsOnGateway(context) {
		generatorLog.Warnf("%s", warning)
	}
	for _, server := range context.Gateway.Spec.Servers {
		if server.Port == nil {
			continue
		}
		if !protocol.GetLayer7ProtocolFromPortName(server.Port.Name).IsMetaProtocol() {
			continue
		}
		port := trans2Port(server)
		outboundProxy, err := buildOutboundProxy(context, port)
		if err != nil {
			return nil, err
		}
		envoyfilters = append(envoyfilters,
			envoyfilter.GenerateReplaceNetworkFilter(
				context.ServiceEntry,
				port,
				outboundProxy,
				nil,
				"envoy.filters.network.meta_protocol_proxy",
				"type.googleapis.com/aeraki.meta_protocol_proxy.v1alpha.MetaProtocolProxy")...)
		// append workloadSelector for OutboundListener EnvoyFilter
		for i := range envoyfilters {
			envoyfilters[i].Name = fmt.Sprintf("aeraki-gateway-outbound-%s.%s-%d", context.Gateway.Name,
				context.Gateway.Namespace, port.Number)
			envoyfilters[i].Namespace = context.Gateway.Namespace
			if envoyfilters[i].Envoyfilter.WorkloadSelector == nil {
				envoyfilters[i].Envoyfilter.WorkloadSelector = &istionetworking.WorkloadSelector{}
			}
			envoyfilters[i].Envoyfilter.WorkloadSelector.Labels = context.Gateway.Spec.Selector
		}
	}
	return envoyfilters, nil
}

func generateSidecarEnvoyFilters(context *model.EnvoyFilterContext) ([]*model.EnvoyFilterWrapper, error) {
	var envoyfilters []*model.EnvoyFilterWrapper
	// The inbound EnvoyFilter (which carries the rate limit filters) is only emitted when the
	// ServiceEntry selects a workload, otherwise it would override the inbound config of unrelated
	// workloads listening on the same port. Warn when a rate limit policy would therefore never be
	// pushed instead of letting it fail silently.
	for _, warning := range rateLimitWarningsWithoutInboundTarget(context) {
		generatorLog.Warnf("%s", warning)
	}
	for _, port := range context.ServiceEntry.Spec.Ports {
		if !protocol.GetLayer7ProtocolFromPortName(port.Name).IsMetaProtocol() {
			continue
		}
		outboundProxy, err := buildOutboundProxy(context, port)
		if err != nil {
			return nil, err
		}
		inboundProxy, err := buildInboundProxy(context, port)
		if err != nil {
			return nil, err
		}
		envoyfilters = append(envoyfilters,
			envoyfilter.GenerateReplaceNetworkFilter(
				context.ServiceEntry,
				port,
				outboundProxy,
				inboundProxy,
				"envoy.filters.network.meta_protocol_proxy",
				"type.googleapis.com/aeraki.meta_protocol_proxy.v1alpha.MetaProtocolProxy")...)
	}
	return envoyfilters, nil
}

func trans2Port(server *istionetworking.Server) *istionetworking.ServicePort {
	return &istionetworking.ServicePort{
		Number:     server.Port.Number,
		Protocol:   server.Port.Protocol,
		Name:       server.Port.Name,
		TargetPort: server.Port.Number,
	}
}

// rateLimitWarningsOnGateway reports rate limit configurations that cannot take effect on a
// gateway: a gateway only receives an outbound proxy, while the rate limit filters are built for
// the inbound filter chain only. The configuration must not be silently ignored.
func rateLimitWarningsOnGateway(context *model.EnvoyFilterContext) []string {
	if context.MetaRouter == nil {
		return nil
	}
	metaRouter := context.MetaRouter
	var warnings []string
	if metaRouter.Spec.LocalRateLimit != nil {
		warnings = append(warnings, fmt.Sprintf("localRateLimit configured on gateway MetaRouter %s/%s is "+
			"ignored: rate limit filters are only applied to the inbound filter chain of sidecar proxies, "+
			"gateway only generates an outbound proxy", metaRouter.Namespace, metaRouter.Name))
	}
	if metaRouter.Spec.GlobalRateLimit != nil {
		warnings = append(warnings, fmt.Sprintf("globalRateLimit configured on gateway MetaRouter %s/%s is "+
			"ignored: rate limit filters are only applied to the inbound filter chain of sidecar proxies, "+
			"gateway only generates an outbound proxy", metaRouter.Namespace, metaRouter.Name))
	}
	return warnings
}

// rateLimitWarningsWithoutInboundTarget reports rate limit policies that cannot reach any proxy on a
// sidecar because the ServiceEntry selects no workload: in that case no inbound EnvoyFilter is
// generated (an inbound patch without a workload selector would affect unrelated workloads on the
// same port).
func rateLimitWarningsWithoutInboundTarget(context *model.EnvoyFilterContext) []string {
	if context.MetaRouter == nil {
		return nil
	}
	metaRouter := context.MetaRouter
	if metaRouter.Spec.LocalRateLimit == nil && metaRouter.Spec.GlobalRateLimit == nil {
		return nil
	}
	selector := envoyfilter.InboundEnvoyFilterWorkloadSelector(context.ServiceEntry)
	if envoyfilter.HasInboundWorkloadSelector(selector) {
		return nil
	}
	serviceName := context.ServiceEntry.Name
	if len(context.ServiceEntry.Spec.Hosts) > 0 {
		serviceName = context.ServiceEntry.Spec.Hosts[0]
	}
	var warnings []string
	if metaRouter.Spec.LocalRateLimit != nil {
		warnings = append(warnings, fmt.Sprintf("localRateLimit of MetaRouter %s/%s for service %s will not take "+
			"effect: the ServiceEntry has neither a workloadSelector nor a workloadSelector annotation, so no "+
			"inbound EnvoyFilter is generated", metaRouter.Namespace, metaRouter.Name, serviceName))
	}
	if metaRouter.Spec.GlobalRateLimit != nil {
		warnings = append(warnings, fmt.Sprintf("globalRateLimit of MetaRouter %s/%s for service %s will not take "+
			"effect: the ServiceEntry has neither a workloadSelector nor a workloadSelector annotation, so no "+
			"inbound EnvoyFilter is generated", metaRouter.Namespace, metaRouter.Name, serviceName))
	}
	return warnings
}
