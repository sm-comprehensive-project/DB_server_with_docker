FROM golang:1.21-alpine AS builder

# librdkafka와 개발 도구 설치
RUN apk add --no-cache \
    gcc \
    musl-dev \
    librdkafka-dev \
    pkgconf

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO 활성화하여 빌드
RUN CGO_ENABLED=1 go build -o main .

# 런타임 이미지
FROM alpine:latest
RUN apk --no-cache add librdkafka
WORKDIR /root/
COPY --from=builder /app/main .
CMD ["./main"]