FROM golang:1.19.2-alpine AS build
ARG GO_OS="linux"
ARG GO_ARCH="amd64"

# see: https://stackoverflow.com/questions/42500973/compiling-go-library-without-gco-to-run-on-alpine-error-in-libczmq
RUN apk add --no-cache czmq-dev build-base util-linux-dev

WORKDIR /build/
COPY . .

# Build binary output
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o sensibled -ldflags '-s -w' main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o strip_block -ldflags '-s -w' tools/strip_block/main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o rewrite_utxo -ldflags '-s -w' tools/rewrite_utxo/main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o rewrite_balance -ldflags '-s -w' tools/rewrite_balance/main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o submit_blocks -ldflags '-s -w' tools/submit_blocks/main.go
RUN GOPROXY=https://goproxy.cn,direct GOOS=${GO_OS} GOARCH=${GO_ARCH} go build -o delete_ft_utxo -ldflags '-s -w' tools/delete_ft_utxo/main.go


FROM alpine:latest
RUN apk add tzdata && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo "Asia/Shanghai" > /etc/timezone
RUN apk add --no-cache libzmq czmq-dev libsodium

RUN adduser -u 1000 -D sato -h /data
USER sato
WORKDIR /data/

COPY --chown=sato --from=build /build/sensibled /data/sensibled
COPY --chown=sato --from=build /build/strip_block /data/strip_block
COPY --chown=sato --from=build /build/rewrite_utxo /data/rewrite_utxo
COPY --chown=sato --from=build /build/rewrite_balance /data/rewrite_balance
COPY --chown=sato --from=build /build/submit_blocks /data/submit_blocks
COPY --chown=sato --from=build /build/delete_ft_utxo /data/delete_ft_utxo

ENTRYPOINT ["./sensibled"]
