# Multi-stage build for the Go pipeline commands. One image carries all the
# binaries; each compose service runs a different one via `command`.
# CGO_ENABLED=0 works because the SQLite driver is pure Go (modernc.org/sqlite).
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/producer  ./cmd/producer  \
 && CGO_ENABLED=0 go build -o /out/validator ./cmd/validator \
 && CGO_ENABLED=0 go build -o /out/projector ./cmd/projector \
 && CGO_ENABLED=0 go build -o /out/cli       ./cmd/cli

# distroless static: tiny, and ships CA certs so the producer's HTTPS call to
# the Wikimedia stream works.
FROM gcr.io/distroless/static-debian12 AS run
COPY --from=build /out/ /app/
# Default; each compose service overrides with its own `command`.
# Use CMD (not ENTRYPOINT) so `command` REPLACES it rather than appending args.
CMD ["/app/producer"]
