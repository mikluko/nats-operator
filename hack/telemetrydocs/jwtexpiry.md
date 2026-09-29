
### Account JWT expiry

The auth controller re-signs each account JWT at half its `jwtTTL`, so while it runs, no account JWT expires sooner than half the shortest `jwtTTL` from now: 24h under the default 48h. Scraped through the chart's ServiceMonitor, this fires once one expires within 23h:

```promql
min(%[1]s) - time() < 23 * 3600
```

The threshold assumes the default 48h `jwtTTL`; a shorter `jwtTTL` needs one below half of it.

The gauge is exported by the auth controller itself and goes stale once its scrape fails, so that expression returns nothing while the auth controller is down. This fires then, from the ServiceMonitor's endpoint `otel-metrics`:

```promql
absent(up{job=~".+-auth-controller-metrics", endpoint="otel-metrics"} == 1)
```
