## Environment

| Variable | Default | Effect |
|---|---|---|
| `OTEL_SDK_DISABLED` | `false` | `true`, in any case, exports nothing whatever else is set; no exporter is built and no Prometheus listener opened. |
| `OTEL_SERVICE_NAME` | the controller's name, such as `cluster-controller` | `service.name` of every metric and span. |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Further resource attributes, as `key=value` pairs separated by commas. |
| `OTEL_METRICS_EXPORTER` | `otlp` once metrics are on | `otlp`, `prometheus`, `console` or `none`; set to any of them, it turns metrics on. |
| `OTEL_TRACES_EXPORTER` | `otlp` once traces are on | `otlp`, `console` or `none`; set to any of them, it turns traces on. |
| `OTEL_EXPORTER_OTLP_PROTOCOL`<br>`OTEL_EXPORTER_OTLP_METRICS_PROTOCOL`<br>`OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `http/protobuf` | `http/protobuf` or `grpc`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT`<br>`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`<br>`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `https://localhost:4318` over `http/protobuf`, `https://localhost:4317` over `grpc` | Where OTLP is sent; set, it turns on the signals it applies to. An `http://` endpoint sends without TLS. |
| `OTEL_EXPORTER_OTLP_INSECURE`<br>`OTEL_EXPORTER_OTLP_METRICS_INSECURE`<br>`OTEL_EXPORTER_OTLP_TRACES_INSECURE` | `false` | `true` sends OTLP without TLS. |
| `OTEL_EXPORTER_OTLP_CERTIFICATE`<br>`OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE`<br>`OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE` | unset | PEM file of the CA certificates the collector's certificate is verified against. |
| `OTEL_EXPORTER_OTLP_HEADERS`<br>`OTEL_EXPORTER_OTLP_METRICS_HEADERS`<br>`OTEL_EXPORTER_OTLP_TRACES_HEADERS` | unset | Headers sent with each export, as `key=value` pairs separated by commas. |
| `OTEL_EXPORTER_OTLP_TIMEOUT`<br>`OTEL_EXPORTER_OTLP_METRICS_TIMEOUT`<br>`OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` | unset | Milliseconds an export may take. |
| `OTEL_EXPORTER_OTLP_COMPRESSION`<br>`OTEL_EXPORTER_OTLP_METRICS_COMPRESSION`<br>`OTEL_EXPORTER_OTLP_TRACES_COMPRESSION` | none | `gzip` compresses each export. |
| `OTEL_EXPORTER_PROMETHEUS_HOST`<br>`OTEL_EXPORTER_PROMETHEUS_PORT` | `localhost`, `9464` | Where `OTEL_METRICS_EXPORTER=prometheus` serves `/metrics` for scraping. |
| `OTEL_METRIC_EXPORT_INTERVAL` | unset | Milliseconds between two exports of the `otlp` and `console` metrics exporters, each reading the resources' status; `prometheus` reads it at each scrape. |
| `OTEL_METRIC_EXPORT_TIMEOUT` | unset | Milliseconds a metric export may take. |
| `OTEL_TRACES_SAMPLER`<br>`OTEL_TRACES_SAMPLER_ARG` | `parentbased_always_on` | Which reconcile spans are kept. |

A variable with `METRICS` or `TRACES` in its name applies to that signal alone and overrides the one without.
