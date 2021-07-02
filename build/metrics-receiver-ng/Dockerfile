FROM golang:1.14.3-alpine AS build
WORKDIR /go/src/mhx.at/gitlab/landscape/metrics-receiver-ng
COPY . /go/src/mhx.at/gitlab/landscape/metrics-receiver-ng
RUN apk add git # needed for fetching (internal) go modules from git repository
RUN GOPRIVATE=www.mhx.at go mod vendor
RUN echo $GOPATH
ENV CGO_ENABLED=0
ARG version
RUN go build -ldflags "-X main.version=$version" -o /out/metrics-receiver-ng ./cmd/metrics-receiver-ng

FROM scratch AS bin
COPY --from=build /out/metrics-receiver-ng /
ENTRYPOINT ["/metrics-receiver-ng"]
