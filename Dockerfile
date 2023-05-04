FROM golang:1.19.2-alpine AS build
ARG GO_OS="linux"
ARG GO_ARCH="amd64"

# see: https://stackoverflow.com/questions/42500973/compiling-go-library-without-gco-to-run-on-alpine-error-in-libczmq
RUN apk add --no-cache czmq-dev build-base util-linux-dev

WORKDIR /build/
COPY . .

# Build binary output
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o unisatd -ldflags '-s -w' main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o block_graph -ldflags '-s -w' tools/block_graph/main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o rewrite_utxo -ldflags '-s -w' tools/rewrite_utxo/main.go

FROM alpine:latest
RUN apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone
RUN apk add --no-cache libzmq czmq-dev libsodium

RUN adduser -u 1000 -D sato -h /data
USER sato
WORKDIR /data/

COPY --chown=sato --from=build /build/rewrite_utxo /data/rewrite_utxo
COPY --chown=sato --from=build /build/block_graph /data/block_graph
COPY --chown=sato --from=build /build/unisatd /data/unisatd

ENTRYPOINT ["./unisatd"]
