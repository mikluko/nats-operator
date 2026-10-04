
## The OpenTelemetry Operator

The chart sets two of the variables above, `OTEL_METRICS_EXPORTER` and `OTEL_EXPORTER_PROMETHEUS_HOST`, and only when `metrics.prometheus.enabled` is set.
The [OpenTelemetry Operator](https://opentelemetry.io/docs/platforms/kubernetes/operator/) sets them from an `Instrumentation` in the release namespace, in every pod annotated `instrumentation.opentelemetry.io/inject-sdk: "true"`.
The chart's `podAnnotations` puts that annotation on the controllers' pods.

An `Instrumentation`:

```yaml
apiVersion: opentelemetry.io/v1alpha1
kind: Instrumentation
metadata:
  name: nats-operator
  namespace: nats-operator
spec:
  exporter:
    endpoint: http://otel-collector.observability.svc:4318
```

The chart values that annotate the controllers' pods for it:

```yaml
podAnnotations:
  instrumentation.opentelemetry.io/inject-sdk: "true"
cluster:
  podAnnotations:
    resource.opentelemetry.io/service.name: cluster-controller
auth:
  podAnnotations:
    resource.opentelemetry.io/service.name: auth-controller
jetstream:
  podAnnotations:
    resource.opentelemetry.io/service.name: jetstream-controller
```

`inject-sdk` injects environment variables only ([SDK environment variables only](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/auto-instrumentation/languages/sdk-only.md)):

- `OTEL_EXPORTER_OTLP_ENDPOINT`, from `spec.exporter.endpoint`. It turns metrics and traces on.
- `OTEL_SERVICE_NAME`.
- `OTEL_RESOURCE_ATTRIBUTES`, with the pod's Kubernetes attributes.
- `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`, from `spec.sampler`.
- Every entry of `spec.env`. Any other variable above goes there, such as `OTEL_EXPORTER_OTLP_PROTOCOL: grpc` for a collector's gRPC port.

The controllers send `http/protobuf` unless `OTEL_EXPORTER_OTLP_PROTOCOL` sets another protocol, so `spec.exporter.endpoint` must be the collector's OTLP/HTTP port.

`OTEL_SERVICE_NAME` is the Deployment's name, `<release>-<controller>`, unless the pod annotation `resource.opentelemetry.io/service.name` sets another, as the chart values above do ([Configure resource attributes](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/auto-instrumentation/resource-attributes.md)).
The three controllers' pods share their `app.kubernetes.io/name` and `app.kubernetes.io/instance` labels, so `spec.defaults.useLabelsForResourceAttributes` alone gives all three the same `service.name`.

A collector can run as a sidecar in each controller's pod ([Sidecar injection](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/collector/sidecar-injection.md)), with these three settings:

- an `OpenTelemetryCollector` (`opentelemetry.io/v1beta1`) with `mode: sidecar` in the release namespace, whose `otlp` receiver takes HTTP on port 4318
- `sidecar.opentelemetry.io/inject: "true"` in `podAnnotations`
- `http://localhost:4318` as the `Instrumentation`'s `spec.exporter.endpoint`

The OpenTelemetry Operator reads both annotations from the pod.
On the Deployment, where the chart's `annotations` puts them, the OpenTelemetry Operator does not see them.
The fields and annotations in this section are those of OpenTelemetry Operator v0.159.0 ([`Instrumentation` API reference](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/api/instrumentations.md)).
