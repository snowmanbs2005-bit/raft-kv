# syntax=docker/dockerfile:1

FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod ./
# No go.sum: this project has zero third-party dependencies (standard
# library only), which is itself part of the "implemented from scratch"
# story -- there is nothing to download here.

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/raftnode ./cmd/raftnode
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/raftctl ./cmd/raftctl

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/raftnode /usr/local/bin/raftnode
COPY --from=build /out/raftctl /usr/local/bin/raftctl

ENTRYPOINT ["/usr/local/bin/raftnode"]
