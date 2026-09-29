
## The OpenTelemetry Operator

Of the variables above, the chart sets only `OTEL_METRICS_EXPORTER` and `OTEL_EXPORTER_PROMETHEUS_HOST`, and only under `metrics.prometheus.enabled`. The [OpenTelemetry Operator](https://opentelemetry.io/docs/platforms/kubernetes/operator/) sets them from an `Instrumentation` in the release namespace:

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

into every pod annotated `instrumentation.opentelemetry.io/inject-sdk: "true"`, which the chart's `podAnnotations` carries:

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

`inject-sdk` injects environment only ([SDK environment variables only](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/auto-instrumentation/languages/sdk-only.md)): `OTEL_EXPORTER_OTLP_ENDPOINT` from `spec.exporter.endpoint`, which turns metrics and traces on; `OTEL_SERVICE_NAME`; `OTEL_RESOURCE_ATTRIBUTES` with the pod's Kubernetes attributes; `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` from `spec.sampler`; and every entry of `spec.env`, where any other variable above goes, such as `OTEL_EXPORTER_OTLP_PROTOCOL: grpc` for a collector's gRPC port. The controllers speak `http/protobuf` unless told otherwise, so the endpoint is the collector's OTLP/HTTP port.

`OTEL_SERVICE_NAME` is the Deployment's name, `<release>-<controller>`, unless the pod annotation `resource.opentelemetry.io/service.name` names another, as above ([Configure resource attributes](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/auto-instrumentation/resource-attributes.md)). The three controllers' pods share their `app.kubernetes.io/name` and `app.kubernetes.io/instance` labels, so `spec.defaults.useLabelsForResourceAttributes` alone gives all three one `service.name`.

To send through a collector in each controller's pod instead, create an `OpenTelemetryCollector` (`opentelemetry.io/v1beta1`) with `mode: sidecar` in the release namespace, whose `otlp` receiver takes HTTP on port 4318; add `sidecar.opentelemetry.io/inject: "true"` to `podAnnotations`; and set the `Instrumentation`'s `spec.exporter.endpoint` to `http://localhost:4318` ([Sidecar injection](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/collector/sidecar-injection.md)).

Both annotations are read off the pod: on the Deployment, under the chart's `annotations`, the OpenTelemetry Operator does not see them. The fields and annotations here are those of OpenTelemetry Operator v0.159.0 ([`Instrumentation` API reference](https://github.com/open-telemetry/opentelemetry-operator/blob/v0.159.0/docs/api/instrumentations.md)).
