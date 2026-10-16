# Build context: services/ (access to the replace directives on ../cbl-kit and ../cbl-protos is required).
# Command: docker build -f cbl-payout/Dockerfile -t cbl/payout:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY cbl-kit/ /src/cbl-kit/
COPY cbl-protos/ /src/cbl-protos/
COPY cbl-payout/ /src/cbl-payout/
WORKDIR /src/cbl-payout
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/payout ./cmd/payout

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/payout /app/payout
COPY --from=build /src/cbl-payout/migrations /app/migrations
EXPOSE 8080 9080 9280
ENTRYPOINT ["/app/payout"]
