<!--
# Copyright Aeraki Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->

With the help of Aeraki Mesh, you can manage proprietary protocols in Istio Service Mesh. Just follow the 
below steps:

## Data Plane

Quickly build an Envoy Filter for a proprietary protocol on top of MetaProtocol:

* Implement the [codec interface](https://github.com/aeraki-mesh/meta-protocol-proxy/blob/ac788327239bd794e745ce18b382da858ddf3355/src/meta_protocol_proxy/codec/codec.h#L118) 
. You can refer to [Dubbo codec](https://github.com/aeraki-mesh/meta-protocol-proxy/tree/master/src/application_protocols/dubbo) and 
                    [Thrift codec](https://github.com/aeraki-mesh/meta-protocol-proxy/tree/master/src/application_protocols/thrift) as writing your own implementation.

* Define the protocol with Aeraki ApplicationProtocol CRD.
Below is an example of the Dubbo protocol:
```yaml
apiVersion: metaprotocol.aeraki.io/v1alpha1
kind: ApplicationProtocol
metadata:
  name: dubbo
  namespace: istio-system
spec:
  protocol: dubbo
  codec: aeraki.meta_protocol.codec.dubbo
```

## Control Plane
You don't need to implement the control plane. Aeraki watches services and traffic rules, generate the
 configurations for the sidecar proxies, and send the configurations to the data plane via EnvoyFilter and MetaRoute
  RDS. 

## Protocol selection

Similar to Istio, protocols are identified by service port prefix. Please name service ports with this pattern: 
tcp-metaprotocol-{layer-7-protocol}-xxx. For example, a Dubbo service port could be named tcp-metaprotocol-dubbo. 

## Traffic management

You can change the route via MetaRouter CRD. For example:

* Route the Dubbo requests calling method sayHello to v2:
```yaml
apiVersion: metaprotocol.aeraki.io/v1alpha1
kind: MetaRouter
metadata:
  name: test-metaprotocol-route
spec:
  hosts:
    - org.apache.dubbo.samples.basic.api.demoservice
  routes:
    - name: v2
    - match:
        attributes:
          method:
            exact: sayHello
      route:
        - destination:
            host: org.apache.dubbo.samples.basic.api.demoservice
            subset: v2
```

* Send 20% of the requests to v1 and 80% to v2:
```yaml
apiVersion: metaprotocol.aeraki.io/v1alpha1
kind: MetaRouter
metadata:
  name: test-metaprotocol-route
spec:
  hosts:
    - org.apache.dubbo.samples.basic.api.demoservice
  routes:
    - name: traffic-spilt
      route:
        - destination:
            host: org.apache.dubbo.samples.basic.api.demoservice
            subset: v1
          weight: 20
        - destination:
            host: org.apache.dubbo.samples.basic.api.demoservice
            subset: v2
          weight: 80
```

## Local rate limit

Local rate limiting is configured through the `localRateLimit` section of a MetaRouter. Aeraki
translates it into the `aeraki.meta_protocol.filters.local_ratelimit` MetaProtocol filter.

```yaml
apiVersion: metaprotocol.aeraki.io/v1alpha1
kind: MetaRouter
metadata:
  name: test-metaprotocol-route
spec:
  hosts:
    - org.apache.dubbo.samples.basic.api.demoservice
  localRateLimit:
    # Default bucket applied to requests that match none of the conditions.
    tokenBucket:
      fillInterval: 60s
      maxTokens: 100
      tokensPerFill: 100
    conditions:
      # First condition: requests with attribute user exactly equal to "vip".
      - match:
          attributes:
            user:
              exact: vip
        tokenBucket:
          fillInterval: 1s
          maxTokens: 100
          tokensPerFill: 100
      # Catch-all condition for every other request.
      - tokenBucket:
          fillInterval: 1s
          maxTokens: 10
          tokensPerFill: 10
```

### Condition evaluation order

The conditions are evaluated by the data-plane filter **in the exact order in which they are
declared, and the first matching condition wins**. Its token bucket is applied and the remaining
conditions are not evaluated. Requests matching no condition fall back to the top-level
`tokenBucket`; if no top-level bucket is configured, such requests are not rate limited.
Aeraki preserves the condition order while generating the filter and never reorders conditions, so
declare the more specific conditions before the more general ones (for example, a condition without
a `match`, which matches every request, should be placed last).

### Token bucket fields

* `maxTokens`: maximum number of tokens the bucket can hold; must be greater than 0.
* `fillInterval`: interval at which tokens are added; required.
* `tokensPerFill`: number of tokens added on every fill interval. When unset, Envoy defaults it to
  **1 token per fill** (the data-plane filter reads it with
  `PROTOBUF_GET_WRAPPED_OR_DEFAULT(..., tokens_per_fill, 1)`, and the Envoy
  `envoy.type.v3.TokenBucket` proto documents the same default). Aeraki keeps omitting the field
  when its value is 0, which is equivalent to that Envoy default of 1; an explicit value of 0 does
  not mean "fill zero tokens".

### Where the filter takes effect

The rate limit filters are only appended to the **inbound** filter chain of the service sidecar.
Therefore:

* On a **gateway** MetaRouter (a MetaRouter referencing a gateway), `localRateLimit` and
  `globalRateLimit` do not take effect, because a gateway only gets an outbound proxy. Aeraki logs
  a warning for every ignored rate limit configuration instead of silently dropping it; remove the
  configuration or move it to a sidecar MetaRouter of the target service.
* An inbound EnvoyFilter is only generated when the ServiceEntry selects a workload, either through
  its `workloadSelector` or through the `workloadSelector` annotation. Without either, no inbound
  EnvoyFilter (and hence no rate limit filter) can be pushed, and Aeraki logs a warning naming the
  affected MetaRouter and service. Add a workload selector to the ServiceEntry to make the policy
  effective.

### Invalid configuration handling

At least one of `tokenBucket` and `conditions` must be present, and every condition must carry a
`tokenBucket` (a condition without one can never rate limit anything). If validation fails in the
control plane — for example a resource created directly through the Kubernetes API that bypassed the
admission webhook and is missing `fillInterval` or `tokensPerFill` — Aeraki returns an explicit
error for that MetaRouter and skips the EnvoyFilter generation of its service only; other services
are not affected.


