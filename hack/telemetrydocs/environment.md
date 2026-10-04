## Environment

Table: The environment variables that configure the OpenTelemetry SDK in each controller.

| Variable | Default | Effect |
|---|---|---|
| `OTEL_SDK_DISABLED` | `false` | `true`, in any letter case, turns off every export, whatever else is set. No exporter is built and no Prometheus listener is opened. |
| `OTEL_SERVICE_NAME` | the controller's name, such as `cluster-controller` | `service.name` of every metric and span. |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Further resource attributes, as `key=value` pairs separated by commas. |
| `OTEL_METRICS_EXPORTER` | `otlp` once metrics are on | `otlp`, `prometheus`, `console` or `none`. Any value but `none` turns metrics on. `none` turns metrics off, whatever else is set. |
| `OTEL_TRACES_EXPORTER` | `otlp` once traces are on | `otlp`, `console` or `none`. Any value but `none` turns traces on. `none` turns traces off, whatever else is set. |
| `OTEL_EXPORTER_OTLP_PROTOCOL`<br>`OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`<br>`OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `http/protobuf` | `http/protobuf` or `grpc`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT`<br>`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`<br>`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `https://localhost:4318` over `http/protobuf`, `https://localhost:4317` over `grpc` | Where OTLP is sent. Setting it turns on the signals it applies to. An `http://` endpoint sends without TLS. |
| `OTEL_EXPORTER_OTLP_INSECURE`<br>`OTEL_EXPORTER_OTLP_METRICS_INSECURE`<br>`OTEL_EXPORTER_OTLP_TRACES_INSECURE` | `false` | `true` sends OTLP without TLS. |
| `OTEL_EXPORTER_OTLP_CERTIFICATE`<br>`OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE`<br>`OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE` | unset | A PEM file of the CA certificates that the collector's certificate is verified against. |
| `OTEL_EXPORTER_OTLP_HEADERS`<br>`OTEL_EXPORTER_OTLP_METRICS_HEADERS`<br>`OTEL_EXPORTER_OTLP_TRACES_HEADERS` | unset | Headers sent with each export, as `key=value` pairs separated by commas. |
| `OTEL_EXPORTER_OTLP_TIMEOUT`<br>`OTEL_EXPORTER_OTLP_METRICS_TIMEOUT`<br>`OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` | unset | Milliseconds an export may take. |
| `OTEL_EXPORTER_OTLP_COMPRESSION`<br>`OTEL_EXPORTER_OTLP_METRICS_COMPRESSION`<br>`OTEL_EXPORTER_OTLP_TRACES_COMPRESSION` | none | `gzip` compresses each export. |
| `OTEL_EXPORTER_PROMETHEUS_HOST`<br>`OTEL_EXPORTER_PROMETHEUS_PORT` | `localhost`, `9464` | Where `OTEL_METRICS_EXPORTER=prometheus` serves `/metrics` for scraping. The chart value `metrics.prometheus.enabled` sets `OTEL_METRICS_EXPORTER=prometheus` and `OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0`, and puts port `9464` on the metrics Service and ServiceMonitor. |
| `OTEL_METRIC_EXPORT_INTERVAL` | unset | Milliseconds between two exports of the `otlp` and `console` metrics exporters. Each export reads the resources' status. The `prometheus` exporter reads the status at each scrape instead. |
| `OTEL_METRIC_EXPORT_TIMEOUT` | unset | Milliseconds a metric export may take. |
| `OTEL_TRACES_SAMPLER`<br>`OTEL_TRACES_SAMPLER_ARG` | `parentbased_always_on` | Which reconcile spans are kept. |

A variable with `METRICS` or `TRACES` in its name applies to that signal alone and overrides the one without.
