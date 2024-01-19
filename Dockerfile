FROM debian:12-slim as builder
RUN apt-get update && apt-get install -y build-essential wget
WORKDIR /app
COPY . .

# Set the Go version
ENV GO_VERSION 1.21.6

RUN ARCH=$(uname -m); \
    if [ "$ARCH" = "x86_64" ]; then \
        GOARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        GOARCH="arm64"; \
    else \
        echo "Unsupported architecture: $ARCH"; \
        exit 1; \
    fi; \
    wget https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz \
    && tar -xvf go${GO_VERSION}.linux-${GOARCH}.tar.gz \
    && mv go /usr/local

ENV PATH="/usr/local/go/bin:${PATH}"
RUN go version
RUN make

FROM debian:12-slim
COPY --from=builder /app/bin/otelcontribcol* /otelcol-contrib
COPY otel-collector-config.yml /etc/otel-collector-config.yml

ENTRYPOINT ["./otelcol-contrib"]
CMD [ "--config=/etc/otel-collector-config.yml" ]
EXPOSE 4317 55678 55679