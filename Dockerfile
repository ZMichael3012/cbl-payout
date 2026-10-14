FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/payout ./cmd/payout

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/payout /app/payout
EXPOSE 8080 9080 9280
ENTRYPOINT ["/app/payout"]