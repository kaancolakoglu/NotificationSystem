FROM golang:1.27.4-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
COPY migrations/ migrations/
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM alpine:3.24.4
COPY --from=build /out/ /usr/local/bin/
USER nobody
