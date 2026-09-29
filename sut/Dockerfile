# Reference payments consumer (SUT). Built from its own module so its
# Kafka/Postgres drivers stay out of the crashpoint tool.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/consumer ./consumer

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/consumer /consumer
ENTRYPOINT ["/consumer"]
