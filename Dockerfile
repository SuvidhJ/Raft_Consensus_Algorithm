# Use official Golang image to build the app
FROM golang:1.21-alpine as builder

WORKDIR /app

# Install dependencies (protoc if generating protobufs, otherwise just build)
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux go build -o raft-kv .

# Minimalist runtime container
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/raft-kv .

EXPOSE 50051

CMD ["./raft-kv"]