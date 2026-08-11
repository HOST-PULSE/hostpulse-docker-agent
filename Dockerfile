FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN go build -o agent main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/agent .
CMD ["./agent"]
