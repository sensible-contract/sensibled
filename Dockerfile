FROM golang:1.15-alpine AS build
ARG GO_OS="linux"
ARG GO_ARCH="amd64"
WORKDIR /build/

# see: https://stackoverflow.com/questions/42500973/compiling-go-library-without-gco-to-run-on-alpine-error-in-libczmq
RUN apk add --no-cache czmq-dev build-base util-linux-dev

COPY . .

# Build binary output
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o satoblock -ldflags '-s -w' main.go

FROM alpine:latest
RUN apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone
RUN apk add --no-cache libzmq czmq-dev libsodium

RUN adduser -u 1000 -D sato -h /data
USER sato
WORKDIR /data/

COPY --chown=sato --from=build /build/satoblock /data/satoblock

ENTRYPOINT ["./satoblock"]
