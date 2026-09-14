FROM golang:1.25-alpine AS builder
WORKDIR /app

# Копируем go.mod для скачивания зависимостей
COPY go.mod ./
RUN go mod download

# 🔥 ИСПРАВЛЕНО: Копируем абсолютно все файлы проекта (включая папку services)
COPY . .

# Компилируем проект (теперь все пакеты на месте)
RUN go build -o agent main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/agent .
CMD ["./agent"]
