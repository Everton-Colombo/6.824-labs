FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY src ./src
COPY cmd ./cmd
RUN go build -o /bin/kvnode ./cmd/kvnode && \
    go build -o /bin/kvbench ./cmd/kvbench

FROM alpine:3.20
RUN apk add --no-cache iproute2 curl
COPY --from=build /bin/kvnode /usr/local/bin/kvnode
COPY --from=build /bin/kvbench /usr/local/bin/kvbench
EXPOSE 9000
ENTRYPOINT ["kvnode"]
