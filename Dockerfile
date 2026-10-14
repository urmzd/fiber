# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mandatum ./cmd/mandatum

FROM alpine:3.22
RUN adduser -D -u 10001 mandatum && mkdir /data && chown mandatum /data
COPY --from=build /out/mandatum /usr/local/bin/mandatum
USER mandatum
ENV MANDATUM_WORKSPACE=/data
VOLUME /data
EXPOSE 8383
ENTRYPOINT ["mandatum"]
CMD ["serve"]
