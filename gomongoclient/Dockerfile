FROM golang:1.25.3-alpine3.22 AS builder
WORKDIR /opt
COPY . .
RUN GO111MODULE=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o gomongoclient .


FROM alpine:3.22
WORKDIR /opt
COPY --from=builder /opt/gomongoclient ./gomongoclient
ENTRYPOINT ["./gomongoclient"]
