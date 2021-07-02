FROM golang:1.14.3-alpine AS build
WORKDIR /go/src/mhx.at/gitlab/landscape/metrics-receiver-ng
COPY . /go/src/mhx.at/gitlab/landscape/metrics-receiver-ng

 # git needed for fetching (internal) go modules from git repository
RUN apk add git

# "patch" git config to add authentication using CI_JOB_TOKEN as password
ARG CI_JOB_TOKEN
ENV CI_JOB_TOKEN=$CI_JOB_TOKEN
RUN git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@www.mhx.at/".insteadOf "https://www.mhx.at/"

RUN GOPRIVATE=www.mhx.at go mod vendor
ENV CGO_ENABLED=0
ARG version
RUN go build -ldflags "-X main.version=$version" -o /out/metrics-receiver-ng ./cmd/metrics-receiver-ng

FROM scratch AS bin
COPY --from=build /out/metrics-receiver-ng /
ENTRYPOINT ["/metrics-receiver-ng"]
