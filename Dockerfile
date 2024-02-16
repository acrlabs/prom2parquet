FROM golang:1.22-alpine

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
RUN chmod +x /usr/local/bin/dumb-init

RUN go install github.com/go-delve/delve/cmd/dlv@latest

COPY prom2parquet /prom2parquet

ENTRYPOINT ["/usr/local/bin/dumb-init", "--"]
