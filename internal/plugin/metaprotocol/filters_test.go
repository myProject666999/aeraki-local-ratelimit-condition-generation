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
	"testing"

	userapi "github.com/aeraki-mesh/api/metaprotocol/v1alpha1"
	mpclient "github.com/aeraki-mesh/client-go/pkg/apis/metaprotocol/v1alpha1"
	lrldpl "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/filters/local_ratelimit/v1alpha"
	mpdataplane "github.com/aeraki-mesh/meta-protocol-control-plane-api/aeraki/meta_protocol_proxy/v1alpha"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	localRateLimitFilterName = "aeraki.meta_protocol.filters.local_ratelimit"
	testHost                 = "org.apache.dubbo.samples.basic.api.demoservice"
)

func tokenBucket(maxTokens, tokensPerFill uint32, seconds int64) *userapi.LocalRateLimit_TokenBucket {
	return &userapi.LocalRateLimit_TokenBucket{
		MaxTokens:     maxTokens,
		TokensPerFill: &wrappers.UInt32Value{Value: tokensPerFill},
		FillInterval:  &duration.Duration{Seconds: seconds},
	}
}

func metaRouterWithLocalRateLimit(limit *userapi.LocalRateLimit) *mpclient.MetaRouter {
	return &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-metarouter",
			Namespace: "meta-dubbo",
		},
		Spec: userapi.MetaRouter{
			Hosts:          []string{testHost},
			LocalRateLimit: limit,
		},
	}
}

// findLocalRateLimitFilter locates the local rate limit filter and unmarshals its anypb config back
// into the data-plane message. No cluster and no real Envoy is involved.
func findLocalRateLimitFilter(t *testing.T,
	filters []*mpdataplane.MetaProtocolFilter) *lrldpl.LocalRateLimit {
	t.Helper()
	for _, filter := range filters {
		if filter.Name != localRateLimitFilterName {
			continue
		}
		if filter.Config == nil {
			t.Fatal("local rate limit filter has a nil config")
		}
		cfg := &lrldpl.LocalRateLimit{}
		if err := filter.Config.UnmarshalTo(cfg); err != nil {
			t.Fatalf("failed to unmarshal local rate limit config: %v", err)
		}
		return cfg
	}
	t.Fatal("local rate limit filter not found")
	return nil
}

// TestBuildInboundFiltersLocalRateLimitOrderAndValues verifies that conditions keep their CRD order
// (the data-plane filter applies the first matching condition, so generation must not reorder) and
// that every bucket value is converted unchanged.
func TestBuildInboundFiltersLocalRateLimitOrderAndValues(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		TokenBucket: tokenBucket(1000, 1000, 60),
		Conditions: []*userapi.LocalRateLimit_Condition{
			{
				Match: &userapi.MetaRouteMatch{
					Attributes: map[string]*userapi.StringMatch{
						"user": {MatchType: &userapi.StringMatch_Exact{Exact: "vip"}},
					},
				},
				TokenBucket: tokenBucket(100, 100, 1),
			},
			{
				// No match: matches every request and acts as the fallback condition.
				TokenBucket: tokenBucket(10, 10, 1),
			},
		},
	})

	filters, err := buildInboundFilters(metaRouter, testHost)
	if err != nil {
		t.Fatalf("buildInboundFilters returned error: %v", err)
	}

	cfg := findLocalRateLimitFilter(t, filters)

	if cfg.StatPrefix != testHost {
		t.Errorf("statPrefix = %q, want %q", cfg.StatPrefix, testHost)
	}

	// The local rate limit filter must be placed before the common filters.
	if len(filters) < 2 || filters[0].Name != localRateLimitFilterName {
		t.Fatalf("local rate limit filter must be the first filter, got: %v", filterNames(filters))
	}

	if cfg.TokenBucket == nil {
		t.Fatal("default tokenBucket should be present")
	}
	if cfg.TokenBucket.MaxTokens != 1000 || cfg.TokenBucket.FillInterval.GetSeconds() != 60 ||
		cfg.TokenBucket.GetTokensPerFill().GetValue() != 1000 {
		t.Errorf("default tokenBucket converted incorrectly: %v", cfg.TokenBucket)
	}

	if len(cfg.Conditions) != 2 {
		t.Fatalf("conditions count = %d, want 2", len(cfg.Conditions))
	}

	first := cfg.Conditions[0]
	if first.TokenBucket.MaxTokens != 100 || first.TokenBucket.GetTokensPerFill().GetValue() != 100 ||
		first.TokenBucket.FillInterval.GetSeconds() != 1 {
		t.Errorf("condition[0] tokenBucket = %v, want maxTokens=100 tokensPerFill=100 fillInterval=1s",
			first.TokenBucket)
	}
	if len(first.Match.GetMetadata()) != 1 {
		t.Fatalf("condition[0] should carry one metadata matcher, got %d", len(first.Match.GetMetadata()))
	}
	matcher := first.Match.GetMetadata()[0]
	if matcher.GetName() != "user" || matcher.GetExactMatch() != "vip" {
		t.Errorf("condition[0] matcher = %v, want user exact vip", matcher)
	}

	second := cfg.Conditions[1]
	if second.TokenBucket.MaxTokens != 10 || second.TokenBucket.GetTokensPerFill().GetValue() != 10 ||
		second.TokenBucket.FillInterval.GetSeconds() != 1 {
		t.Errorf("condition[1] tokenBucket = %v, want maxTokens=10 tokensPerFill=10 fillInterval=1s",
			second.TokenBucket)
	}
	if len(second.Match.GetMetadata()) != 0 {
		t.Errorf("condition[1] should have an empty match (match all), got %v", second.Match.GetMetadata())
	}
}

// TestBuildInboundFiltersConditionWithoutTokenBucketRejected fixes the chosen behavior for a
// condition without a tokenBucket: the whole generation fails with an explicit error instead of
// silently skipping the item. A later valid condition must not hide the invalid one.
func TestBuildInboundFiltersConditionWithoutTokenBucketRejected(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		Conditions: []*userapi.LocalRateLimit_Condition{
			{
				Match: &userapi.MetaRouteMatch{
					Attributes: map[string]*userapi.StringMatch{
						"user": {MatchType: &userapi.StringMatch_Exact{Exact: "vip"}},
					},
				},
				TokenBucket: tokenBucket(100, 100, 1),
			},
			{
				// No match: matches every request and acts as the fallback condition.
				TokenBucket: tokenBucket(10, 10, 1),
			},
			{
				Match: &userapi.MetaRouteMatch{
					Attributes: map[string]*userapi.StringMatch{
						"user": {MatchType: &userapi.StringMatch_Exact{Exact: "guest"}},
					},
				},
				// TokenBucket intentionally missing.
			},
		},
	})

	if _, err := buildInboundFilters(metaRouter, testHost); err == nil {
		t.Fatal("expected an error when a condition has no tokenBucket, got nil")
	}
}

// TestBuildInboundFiltersOnlyBodilessConditionRejected covers the previously silent no-op case: the
// top-level "tokenBucket or conditions" check passed while the generated filter had neither.
func TestBuildInboundFiltersOnlyBodilessConditionRejected(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		Conditions: []*userapi.LocalRateLimit_Condition{
			{
				Match: &userapi.MetaRouteMatch{
					Attributes: map[string]*userapi.StringMatch{
						"user": {MatchType: &userapi.StringMatch_Exact{Exact: "vip"}},
					},
				},
			},
		},
	})

	if _, err := buildInboundFilters(metaRouter, testHost); err == nil {
		t.Fatal("expected an error when the only condition has no tokenBucket, got nil")
	}
}

// TestBuildInboundFiltersNilTokenBucketFieldsReturnError ensures the controller path cannot panic
// on resources that bypassed the webhook.
func TestBuildInboundFiltersNilTokenBucketFieldsReturnError(t *testing.T) {
	testCases := []struct {
		name  string
		limit *userapi.LocalRateLimit
	}{
		{
			name: "nil fill interval on default bucket",
			limit: &userapi.LocalRateLimit{
				TokenBucket: &userapi.LocalRateLimit_TokenBucket{
					MaxTokens:     10,
					TokensPerFill: &wrappers.UInt32Value{Value: 10},
				},
			},
		},
		{
			name: "nil tokens per fill on default bucket",
			limit: &userapi.LocalRateLimit{
				TokenBucket: &userapi.LocalRateLimit_TokenBucket{
					MaxTokens:    10,
					FillInterval: &duration.Duration{Seconds: 1},
				},
			},
		},
		{
			name: "nil fill interval on condition bucket",
			limit: &userapi.LocalRateLimit{
				Conditions: []*userapi.LocalRateLimit_Condition{
					{
						TokenBucket: &userapi.LocalRateLimit_TokenBucket{
							MaxTokens:     10,
							TokensPerFill: &wrappers.UInt32Value{Value: 10},
						},
					},
				},
			},
		},
		{
			name: "nil tokens per fill on condition bucket",
			limit: &userapi.LocalRateLimit{
				Conditions: []*userapi.LocalRateLimit_Condition{
					{
						TokenBucket: &userapi.LocalRateLimit_TokenBucket{
							MaxTokens:    10,
							FillInterval: &duration.Duration{Seconds: 1},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metaRouter := metaRouterWithLocalRateLimit(tc.limit)
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("generation panicked instead of returning an error: %v", r)
				}
			}()
			if _, err := buildInboundFilters(metaRouter, testHost); err == nil {
				t.Fatal("expected an error for incomplete tokenBucket, got nil")
			}
		})
	}
}

// TestBuildInboundFiltersEmptyHostReturnsError ensures an empty host (for example a MetaRouter
// without hosts) produces an error instead of an index out of range panic.
func TestBuildInboundFiltersEmptyHostReturnsError(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		TokenBucket: tokenBucket(10, 10, 1),
	})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("generation panicked instead of returning an error: %v", r)
		}
	}()
	if _, err := buildInboundFilters(metaRouter, ""); err == nil {
		t.Fatal("expected an error for an empty host, got nil")
	}
}

// TestBuildInboundFiltersNoRateLimitKeepsCommonFilters verifies that MetaRouters without rate limit
// configuration still get the common filters and no local rate limit filter.
func TestBuildInboundFiltersNoRateLimitKeepsCommonFilters(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(nil)

	filters, err := buildInboundFilters(metaRouter, testHost)
	if err != nil {
		t.Fatalf("buildInboundFilters returned error: %v", err)
	}
	for _, filter := range filters {
		if filter.Name == localRateLimitFilterName {
			t.Fatal("local rate limit filter should not be generated without configuration")
		}
	}
	if len(filters) != 3 {
		t.Fatalf("common filters count = %d, want 3: %v", len(filters), filterNames(filters))
	}
}

// TestCrd2TokenBucketZeroTokensPerFillOmitted locks the historical behavior: a zero tokensPerFill
// is not emitted on the wire, and Envoy applies its documented default of 1 token per fill.
func TestCrd2TokenBucketZeroTokensPerFillOmitted(t *testing.T) {
	bucket, err := crd2tokenBucket(tokenBucket(10, 0, 1))
	if err != nil {
		t.Fatalf("crd2tokenBucket returned error: %v", err)
	}
	if bucket.TokensPerFill != nil {
		t.Errorf("tokensPerFill should be omitted when zero, got %v", bucket.TokensPerFill)
	}
	if bucket.MaxTokens != 10 || bucket.FillInterval.GetSeconds() != 1 {
		t.Errorf("bucket converted incorrectly: %v", bucket)
	}
}

// TestCrd2ConditionsPreservesOrder independently verifies that condition conversion never reorders.
func TestCrd2ConditionsPreservesOrder(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(nil)
	conditions := []*userapi.LocalRateLimit_Condition{
		{
			Match: &userapi.MetaRouteMatch{
				Attributes: map[string]*userapi.StringMatch{
					"user": {MatchType: &userapi.StringMatch_Exact{Exact: "first"}},
				},
			},
			TokenBucket: tokenBucket(1, 1, 1),
		},
		{
			Match: &userapi.MetaRouteMatch{
				Attributes: map[string]*userapi.StringMatch{
					"user": {MatchType: &userapi.StringMatch_Exact{Exact: "second"}},
				},
			},
			TokenBucket: tokenBucket(2, 2, 2),
		},
		{
			Match: &userapi.MetaRouteMatch{
				Attributes: map[string]*userapi.StringMatch{
					"user": {MatchType: &userapi.StringMatch_Exact{Exact: "third"}},
				},
			},
			TokenBucket: tokenBucket(3, 3, 3),
		},
	}

	got, err := crd2Conditions(metaRouter, conditions)
	if err != nil {
		t.Fatalf("crd2Conditions returned error: %v", err)
	}
	wantExact := []string{"first", "second", "third"}
	for i, want := range wantExact {
		if got[i].Match.GetMetadata()[0].GetExactMatch() != want {
			t.Errorf("condition[%d] exact = %q, want %q: condition order must be preserved",
				i, got[i].Match.GetMetadata()[0].GetExactMatch(), want)
		}
	}
}

// TestAnyUnmarshalRoundTrip checks that the marshaled anypb config unmarshals back into an equal
// data-plane message, which is how the generated EnvoyFilter is consumed.
func TestAnyUnmarshalRoundTrip(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		TokenBucket: tokenBucket(100, 100, 1),
		Conditions: []*userapi.LocalRateLimit_Condition{
			{
				Match: &userapi.MetaRouteMatch{
					Attributes: map[string]*userapi.StringMatch{
						"user": {MatchType: &userapi.StringMatch_Exact{Exact: "vip"}},
					},
				},
				TokenBucket: tokenBucket(10, 10, 1),
			},
		},
	})

	filters, err := buildInboundFilters(metaRouter, testHost)
	if err != nil {
		t.Fatalf("buildInboundFilters returned error: %v", err)
	}
	cfg := findLocalRateLimitFilter(t, filters)
	if cfg.GetStatPrefix() != testHost {
		t.Errorf("statPrefix = %q, want %q", cfg.GetStatPrefix(), testHost)
	}

	// Re-marshal and unmarshal through anypb to prove the Any is self-describing and decodable.
	raw, err := anypb.New(cfg)
	if err != nil {
		t.Fatalf("anypb.New failed: %v", err)
	}
	decoded := &lrldpl.LocalRateLimit{}
	if err := raw.UnmarshalTo(decoded); err != nil {
		t.Fatalf("UnmarshalTo failed: %v", err)
	}
	if !proto.Equal(decoded, cfg) {
		t.Errorf("round trip changed the message: got %v want %v", decoded, cfg)
	}
}

func filterNames(filters []*mpdataplane.MetaProtocolFilter) []string {
	names := make([]string, 0, len(filters))
	for _, filter := range filters {
		names = append(names, filter.Name)
	}
	return names
}

// TestBuildInboundFiltersErrorIsolation simulates the controller loop over multiple services: a
// broken MetaRouter returns an error for its own service while another MetaRouter still generates
// its filters, so one bad configuration cannot break EnvoyFilter generation for other services.
func TestBuildInboundFiltersErrorIsolation(t *testing.T) {
	broken := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		// Webhook would reject this; emulates a resource created via the Kubernetes API directly.
		TokenBucket: &userapi.LocalRateLimit_TokenBucket{
			MaxTokens: 10,
			// FillInterval and TokensPerFill both nil.
		},
	})
	healthy := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		TokenBucket: tokenBucket(10, 10, 1),
	})

	if _, err := buildInboundFilters(broken, testHost); err == nil {
		t.Fatal("broken MetaRouter should return an error")
	}

	filters, err := buildInboundFilters(healthy, testHost)
	if err != nil {
		t.Fatalf("healthy MetaRouter should still generate filters, got error: %v", err)
	}
	if findLocalRateLimitFilter(t, filters) == nil {
		t.Fatal("healthy MetaRouter should contain a local rate limit filter")
	}
}

// TestBuildInboundFiltersGlobalAfterLocalError verifies the local rate limit error aborts filter
// building before the common filters are appended, so no half-built filter chain is returned.
func TestBuildInboundFiltersGlobalAfterLocalError(t *testing.T) {
	metaRouter := metaRouterWithLocalRateLimit(&userapi.LocalRateLimit{
		Conditions: []*userapi.LocalRateLimit_Condition{
			{
				// TokenBucket missing: must be rejected rather than silently skipped.
				Match: &userapi.MetaRouteMatch{
					Attributes: map[string]*userapi.StringMatch{
						"user": {MatchType: &userapi.StringMatch_Exact{Exact: "vip"}},
					},
				},
			},
		},
	})

	filters, err := buildInboundFilters(metaRouter, testHost)
	if err == nil {
		t.Fatal("expected an error for a condition without tokenBucket")
	}
	if len(filters) != 0 {
		t.Errorf("no filters should be returned on error, got %v", filterNames(filters))
	}
}

// TestBuildInboundFiltersGlobalRateLimitNilTimeoutReturnsError guards the controller path against
// a panic when a globalRateLimit resource bypassed the webhook without requestTimeout.
func TestBuildInboundFiltersGlobalRateLimitNilTimeoutReturnsError(t *testing.T) {
	metaRouter := &mpclient.MetaRouter{
		ObjectMeta: metav1.ObjectMeta{Name: "test-metarouter", Namespace: "meta-dubbo"},
		Spec: userapi.MetaRouter{
			Hosts: []string{testHost},
			GlobalRateLimit: &userapi.GlobalRateLimit{
				Descriptors: []*userapi.GlobalRateLimit_Descriptor{
					{Property: "user", DescriptorKey: "user"},
				},
				// RequestTimeout intentionally missing.
			},
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("generation panicked instead of returning an error: %v", r)
		}
	}()
	if _, err := buildInboundFilters(metaRouter, testHost); err == nil {
		t.Fatal("expected an error for globalRateLimit without requestTimeout, got nil")
	}
}
