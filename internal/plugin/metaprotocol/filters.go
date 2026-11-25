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

	userapi "github.com/aeraki-mesh/api/metaprotocol/v1alpha1"
	mpclient "github.com/aeraki-mesh/client-go/pkg/apis/metaprotocol/v1alpha1"
	metaroute "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/config/route/v1alpha"

	//nolint
	grldpl "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/filters/global_ratelimit/v1alpha"

	stats "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/filters/istio_stats/v1alpha"

	//nolint
	lrldpl "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/filters/local_ratelimit/v1alpha"

	mpdataplane "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/v1alpha"
	envoycore "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoyrl "github.com/envoyproxy/go-control-plane/envoy/config/ratelimit/v3"
	commondataplane "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/aeraki-mesh/aeraki/internal/xds"
)

func buildOutboundFilters(host string) []*mpdataplane.MetaProtocolFilter {
	var filters []*mpdataplane.MetaProtocolFilter
	filters = appendCommonFilters(filters, host)
	return filters
}

func buildInboundFilters(metaRouter *mpclient.MetaRouter, host string) ([]*mpdataplane.MetaProtocolFilter, error) {
	var filters []*mpdataplane.MetaProtocolFilter
	var err error
	if metaRouter != nil {
		if metaRouter.Spec.LocalRateLimit != nil {
			if filters, err = appendLocalRateLimitFilter(metaRouter, host, filters); err != nil {
				return filters, err
			}
		}
		if metaRouter.Spec.GlobalRateLimit != nil {
			if filters, err = appendGlobalRateLimitFilter(metaRouter, filters); err != nil {
				return filters, err
			}
		}
	}

	return appendCommonFilters(filters, host), nil
}

func appendCommonFilters(filters []*mpdataplane.MetaProtocolFilter, host string) []*mpdataplane.MetaProtocolFilter {
	metadataFilter := mpdataplane.MetaProtocolFilter{
		Name: "aeraki.meta_protocol.filters.metadata_exchange",
	}
	stats := &stats.IstioStats{
		DestinationService: host,
	}
	config, err := anypb.New(stats)
	if err != nil {
		generatorLog.Errorf("stats create failed: %e", err)
	}

	statsFilter := mpdataplane.MetaProtocolFilter{
		Name:   "aeraki.meta_protocol.filters.istio_stats",
		Config: config,
	}

	router := mpdataplane.MetaProtocolFilter{
		Name: "aeraki.meta_protocol.filters.router",
	}
	return append(filters, &metadataFilter, &statsFilter, &router)
}

// appendLocalRateLimitFilter converts the localRateLimit section of a MetaRouter into the
// aeraki.meta_protocol.filters.local_ratelimit filter.
//
// Condition ordering is significant and must be preserved: the data-plane filter iterates the
// conditions in list order and applies the token bucket of the first matching condition
// (first match wins); requests matching none of the conditions fall back to the default
// tokenBucket. Hence conditions are converted one by one in their original order and never
// reordered.
func appendLocalRateLimitFilter(metaRouter *mpclient.MetaRouter, host string,
	filters []*mpdataplane.MetaProtocolFilter) ([]*mpdataplane.MetaProtocolFilter, error) {
	localRateLimit := metaRouter.Spec.LocalRateLimit

	if localRateLimit.TokenBucket == nil && len(localRateLimit.Conditions) == 0 {
		return nil, fmt.Errorf("either tokenBucket or conditions should be specified")
	}
	if host == "" {
		return nil, fmt.Errorf("can not build local rate limit filter for MetaRouter %s/%s: empty host",
			metaRouter.Namespace, metaRouter.Name)
	}

	lrt := &lrldpl.LocalRateLimit{
		StatPrefix: host,
	}
	if localRateLimit.TokenBucket != nil {
		tokenBucket, err := crd2tokenBucket(localRateLimit.TokenBucket)
		if err != nil {
			return nil, fmt.Errorf("invalid localRateLimit tokenBucket: %w", err)
		}
		lrt.TokenBucket = tokenBucket
	}
	if len(localRateLimit.Conditions) > 0 {
		conditions, err := crd2Conditions(metaRouter, localRateLimit.Conditions)
		if err != nil {
			return nil, err
		}
		lrt.Conditions = conditions
	}

	// All conditions may have been dropped because they had no tokenBucket. A filter without a
	// default bucket and without any condition does nothing, so reject it instead of silently
	// generating an ineffective filter.
	if lrt.TokenBucket == nil && len(lrt.Conditions) == 0 {
		return nil, fmt.Errorf("localRateLimit of MetaRouter %s/%s has no valid tokenBucket or condition "+
			"with a tokenBucket", metaRouter.Namespace, metaRouter.Name)
	}

	// Validate the data-plane message before marshaling, so an invalid configuration surfaces as an
	// explicit error for this MetaRouter instead of being rejected (or misbehaving) at the proxy.
	if err := lrt.Validate(); err != nil {
		return nil, fmt.Errorf("invalid local rate limit filter config for MetaRouter %s/%s: %w",
			metaRouter.Namespace, metaRouter.Name, err)
	}

	config, err := anypb.New(lrt)
	if err != nil {
		// Do not append a filter with a nil config; return an error so that only this MetaRouter's
		// service is affected, rather than pushing a broken EnvoyFilter.
		return nil, fmt.Errorf("failed to marshal local rate limit filter config for MetaRouter %s/%s: %w",
			metaRouter.Namespace, metaRouter.Name, err)
	}

	localRateLimitFilter := mpdataplane.MetaProtocolFilter{
		Name:   "aeraki.meta_protocol.filters.local_ratelimit",
		Config: config,
	}
	filters = append(filters, &localRateLimitFilter)
	return filters, nil
}

func appendGlobalRateLimitFilter(metaRouter *mpclient.MetaRouter,
	filters []*mpdataplane.MetaProtocolFilter) ([]*mpdataplane.MetaProtocolFilter, error) {
	globalRateLimit := metaRouter.Spec.GlobalRateLimit

	if len(globalRateLimit.Descriptors) == 0 {
		return nil, fmt.Errorf("the length of global rate limit actions should not be zero")
	}
	if globalRateLimit.RequestTimeout == nil {
		return nil, fmt.Errorf("globalRateLimit requestTimeout is required")
	}

	var descriptors []*grldpl.Descriptor
	for _, action := range globalRateLimit.Descriptors {
		descriptors = append(descriptors, &grldpl.Descriptor{
			Property:      action.Property,
			DescriptorKey: action.DescriptorKey,
		})
	}

	grt := &grldpl.RateLimit{
		Match: &metaroute.RouteMatch{
			Metadata: xds.MetaMatch2HttpHeaderMatch(globalRateLimit.Match),
		},
		Domain: globalRateLimit.Domain,
		Timeout: &duration.Duration{
			Seconds: globalRateLimit.RequestTimeout.Seconds,
			Nanos:   globalRateLimit.RequestTimeout.Nanos,
		},
		FailureModeDeny: globalRateLimit.DenyOnFail,
		RateLimitService: &envoyrl.RateLimitServiceConfig{
			GrpcService: &envoycore.GrpcService{
				TargetSpecifier: &envoycore.GrpcService_EnvoyGrpc_{
					EnvoyGrpc: &envoycore.GrpcService_EnvoyGrpc{
						ClusterName: globalRateLimit.RateLimitService,
					},
				},
			},
		},
		Descriptors: descriptors,
	}
	config, err := anypb.New(grt)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal global rate limit filter config for MetaRouter %s/%s: %w",
			metaRouter.Namespace, metaRouter.Name, err)
	}

	globalRateLimitFilter := mpdataplane.MetaProtocolFilter{
		Name:   "aeraki.meta_protocol.filters.ratelimit",
		Config: config,
	}
	filters = append(filters, &globalRateLimitFilter)
	return filters, nil
}

// crd2Conditions converts MetaRouter local rate limit conditions into data-plane conditions.
// The conditions are appended in the exact order in which they appear on the MetaRouter so that
// the first-match-wins semantics of the data-plane filter is preserved (the filter evaluates the
// conditions in order and uses the token bucket of the first matching condition).
//
// A condition without a tokenBucket can never rate limit anything and is rejected with an error,
// which fails the generation for the affected MetaRouter instead of silently dropping the item.
func crd2Conditions(metaRouter *mpclient.MetaRouter,
	conditions []*userapi.LocalRateLimit_Condition) ([]*lrldpl.LocalRateLimitCondition, error) {
	localConditions := make([]*lrldpl.LocalRateLimitCondition, 0, len(conditions))
	for i, condition := range conditions {
		if condition == nil {
			return nil, fmt.Errorf("localRateLimit condition[%d] of MetaRouter %s/%s is nil",
				i, metaRouter.Namespace, metaRouter.Name)
		}
		if condition.TokenBucket == nil {
			return nil, fmt.Errorf("localRateLimit condition[%d] of MetaRouter %s/%s has no tokenBucket",
				i, metaRouter.Namespace, metaRouter.Name)
		}
		tokenBucket, err := crd2tokenBucket(condition.TokenBucket)
		if err != nil {
			return nil, fmt.Errorf("invalid tokenBucket of localRateLimit condition[%d] of MetaRouter %s/%s: %w",
				i, metaRouter.Namespace, metaRouter.Name, err)
		}
		localConditions = append(localConditions, &lrldpl.LocalRateLimitCondition{
			TokenBucket: tokenBucket,
			Match: &metaroute.RouteMatch{
				Metadata: xds.MetaMatch2HttpHeaderMatch(condition.Match),
			},
		})
	}
	return localConditions, nil
}

// crd2tokenBucket converts a CRD token bucket into the Envoy envoy.type.v3.TokenBucket. All pointer
// fields dereferenced below are checked first so that a resource which bypassed the webhook (for
// example one created directly through the Kubernetes API) produces an explicit error instead of
// panicking and aborting the whole EnvoyFilter generation loop.
func crd2tokenBucket(tbCrd *userapi.LocalRateLimit_TokenBucket) (*commondataplane.TokenBucket, error) {
	if tbCrd == nil {
		return nil, fmt.Errorf("tokenBucket is nil")
	}
	if tbCrd.FillInterval == nil {
		return nil, fmt.Errorf("tokenBucket fillInterval is required")
	}
	if tbCrd.TokensPerFill == nil {
		return nil, fmt.Errorf("tokenBucket tokensPerFill is required")
	}

	tokenBucket := &commondataplane.TokenBucket{
		MaxTokens: tbCrd.MaxTokens,
		FillInterval: &duration.Duration{
			Seconds: tbCrd.FillInterval.Seconds,
			Nanos:   tbCrd.FillInterval.Nanos,
		},
	}

	// Keep the historical behavior of only emitting tokensPerFill when it is non zero. Envoy's
	// envoy.type.v3.TokenBucket treats an absent tokens_per_fill as the default of 1 (the data-plane
	// local_ratelimit filter reads it with PROTOBUF_GET_WRAPPED_OR_DEFAULT(..., tokens_per_fill, 1)),
	// and the CRD field documents the same default, so omitting the field at value 0 is equivalent
	// to setting it to 1 on the Envoy side. Do not change this without re-verifying that default.
	if tokensPerFill := tbCrd.TokensPerFill.Value; tokensPerFill != 0 {
		tokenBucket.TokensPerFill = &wrappers.UInt32Value{
			Value: tokensPerFill,
		}
	}
	return tokenBucket, nil
}
