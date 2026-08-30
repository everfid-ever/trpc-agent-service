FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags='-s -w' -o /out/trpc-service ./cmd/trpc-service

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/trpc-service /trpc-service
USER nonroot:nonroot
ENTRYPOINT ["/trpc-service"]
CMD ["artifact"]
