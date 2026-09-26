# One image per controller: docker build --build-arg CONTROLLER=<name>-controller.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG CONTROLLER
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN test -n "$CONTROLLER" && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/controller "./cmd/$CONTROLLER"

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/controller /controller
USER 65532:65532
ENTRYPOINT ["/controller"]
