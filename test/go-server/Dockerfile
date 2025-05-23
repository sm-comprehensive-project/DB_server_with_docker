FROM golang:1.21-bullseye AS builder

# librdkafka와 개발 도구 설치
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# go.mod만 먼저 복사
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
FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /root/
COPY --from=builder /app/main .
CMD ["./main"]