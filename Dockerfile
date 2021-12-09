FROM golang:1.15-alpine AS build
ARG GO_OS="linux"
ARG GO_ARCH="amd64"

# see: https://stackoverflow.com/questions/42500973/compiling-go-library-without-gco-to-run-on-alpine-error-in-libczmq
RUN apk add --no-cache czmq-dev build-base util-linux-dev

WORKDIR /build/
COPY . .

# Build binary output
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o sensibled -ldflags '-s -w' main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o check_mempool_double_spend -ldflags '-s -w' tools/check_mempool_double_spend/main.go

FROM alpine:latest
RUN apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone
RUN apk add --no-cache libzmq czmq-dev libsodium

RUN adduser -u 1000 -D sato -h /data
USER sato
WORKDIR /data/

COPY --chown=sato --from=build /build/sensibled /data/sensibled
COPY --chown=sato --from=build /build/check_mempool_double_spend /data/check_mempool_double_spend

ENTRYPOINT ["./sensibled"]
