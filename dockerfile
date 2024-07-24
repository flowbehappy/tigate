FROM golang:1.21-alpine as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/tigate
COPY . .
ENV CDC_ENABLE_VENDOR=0
RUN make cdc

FROM alpine:3.15
RUN apk add --no-cache tzdata bash curl socat
EXPOSE 8301
EXPOSE 8300
COPY --from=builder /go/src/tigate/bin/cdc /cdc
CMD [ "cdc" ]