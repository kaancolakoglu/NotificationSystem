FROM golang:1.27.4-alpine3.24 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd/ cmd/
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api

FROM alpine:3.24.4
COPY --from=build /out/api /usr/local/bin/api
USER nobody
ENTRYPOINT ["/usr/local/bin/api"]
