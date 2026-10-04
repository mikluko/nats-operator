
### Account JWT expiry

While the auth controller runs, it re-signs each account JWT at half its `jwtTTL`.
No account JWT then expires sooner than half the shortest `jwtTTL` from now, which is 24h under the default `jwtTTL` of 48h.

Scraped through the chart's ServiceMonitor, this expression fires when an account JWT expires within 23h:

```promql
min(%[1]s) - time() < 23 * 3600
```

The threshold of 23h assumes the default `jwtTTL` of 48h.
Under a shorter `jwtTTL`, the threshold must be below half of it.

The auth controller exports the gauge itself, and the gauge goes stale when its scrape fails.
While the auth controller is down, the expression above returns nothing.
This expression fires then, from the ServiceMonitor's endpoint `otel-metrics`:

```promql
absent(up{job=~".+-auth-controller-metrics", endpoint="otel-metrics"} == 1)
```
