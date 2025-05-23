FROM golang:1.21-alpine AS builder

# librdkafka와 개발 도구 설치
RUN apk add --no-cache \
    gcc \
    musl-dev \
    librdkafka-dev \
    pkgconf

WORKDIR /app

# go.mod만 먼저 복사 (go.sum이 없어도 됨)
COPY go.mod ./

# go.sum이 있으면 복사, 없으면 무시
COPY go.su[m] ./

# 모듈 다운로드 및 go.sum 생성
RUN go mod download
RUN go mod tidy

# 소스 코드 복사
COPY . .

# CGO 활성화하여 빌드
RUN CGO_ENABLED=1 go build -o main .

# 런타임 이미지
FROM alpine:latest
RUN apk --no-cache add librdkafka
WORKDIR /root/
COPY --from=builder /app/main .
CMD ["./main"]