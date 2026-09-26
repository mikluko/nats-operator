# One controller image from a prebuilt static linux binary; the build context
# holds only that binary, named by CONTROLLER.
FROM scratch
ARG CONTROLLER
COPY ${CONTROLLER} /controller
USER 65532:65532
ENTRYPOINT ["/controller"]
