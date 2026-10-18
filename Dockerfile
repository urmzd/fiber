# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fiber ./cmd/fiber

FROM alpine:3.22
RUN adduser -D -u 10001 fiber && mkdir /data && chown fiber /data
COPY --from=build /out/fiber /usr/local/bin/fiber
USER fiber
ENV FIBER_WORKSPACE=/data
VOLUME /data
EXPOSE 8383
ENTRYPOINT ["fiber"]
CMD ["serve"]
