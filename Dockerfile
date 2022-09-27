FROM golang:1.19-alpine AS build
ARG GO_OS="linux"
ARG GO_ARCH="amd64"

# see: https://stackoverflow.com/questions/42500973/compiling-go-library-without-gco-to-run-on-alpine-error-in-libczmq
RUN apk add --no-cache czmq-dev build-base util-linux-dev

WORKDIR /build/
COPY . .

# Build binary output
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o sensibled -ldflags '-s -w' main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o strip_block -ldflags '-s -w' tools/strip_block/main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o fix_utxo -ldflags '-s -w' tools/fix_utxo/main.go

FROM alpine:latest
RUN apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone
RUN apk add --no-cache libzmq czmq-dev libsodium

RUN adduser -u 1000 -D sato -h /data
USER sato
WORKDIR /data/

COPY --chown=sato --from=build /build/sensibled /data/sensibled
COPY --chown=sato --from=build /build/strip_block /data/strip_block
COPY --chown=sato --from=build /build/fix_utxo /data/fix_utxo


ENTRYPOINT ["./sensibled"]
