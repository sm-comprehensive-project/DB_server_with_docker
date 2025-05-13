#!/bin/bash

# MongoDB 인스턴스가 시작될 때까지 대기
echo "Waiting for MongoDB instances to start..."
sleep 30

# MongoDB 관리자 계정 정보
MONGO_ADMIN_USER="admin"
MONGO_ADMIN_PASSWORD="secure_password"
MONGO_HOST="mongo1:27017"

# MongoDB가 접속 가능한지 확인
echo "Checking MongoDB connection..."
until mongosh --host $MONGO_HOST --quiet --eval "db.version()" &>/dev/null; do
  echo "Waiting for MongoDB to be ready..."
  sleep 5
done

# 먼저 인증 없이 admin 사용자 생성 시도 (첫 실행 시)
echo "Attempting to create initial admin user if not exists..."
mongosh --host $MONGO_HOST --eval '
db = db.getSiblingDB("admin");
try {
  db.createUser({
    user: "admin",
    pwd: "secure_password",
    roles: [{ role: "root", db: "admin" }]
  });
  print("Initial admin user created successfully");
} catch(e) {
  if(e.codeName !== "DuplicateKey") {
    print("Note: " + e.message);
  }
}
' &>/dev/null

# 관리자 인증 연결 문자열
AUTH_CONN="--host $MONGO_HOST --authenticationDatabase admin -u $MONGO_ADMIN_USER -p $MONGO_ADMIN_PASSWORD"

# 레플리카셋 상태 확인 (인증 포함)
echo "Checking replica set status..."
RS_STATUS=$(mongosh $AUTH_CONN --quiet --eval "try { rs.status().ok } catch(e) { 0 }" 2>/dev/null || echo "0")

if [ "$RS_STATUS" == "1" ]; then
    echo "Replica set is already initialized"
else
    echo "Initializing replica set..."
    # replicaSet.js 파일 내용 실행 (인증 포함)
    mongosh $AUTH_CONN --eval "$(cat /usr/src/scripts/replicaSet.js)" || {
      # 인증 없이도 시도 (첫 실행 시 인증이 설정되지 않았을 수 있음)
      echo "Trying to initialize replica set without authentication..."
      mongosh --host $MONGO_HOST /usr/src/scripts/replicaSet.js
    }
    
    # 레플리카셋 초기화 후 잠시 대기
    echo "Waiting for replica set to stabilize..."
    sleep 10
fi

# 관리자 계정이 존재하는지 확인 (인증 포함)
echo "Checking if admin user exists..."
ADMIN_EXISTS=$(mongosh $AUTH_CONN --quiet --eval "db.getSiblingDB('admin').getUser('admin') ? 1 : 0" 2>/dev/null || echo "0")

if [ "$ADMIN_EXISTS" == "1" ]; then
    echo "Admin user already exists"
else
    echo "Creating admin user..."
    # 인증 없이 시도 (첫 실행 시)
    mongosh --host $MONGO_HOST --eval '
    db = db.getSiblingDB("admin");
    db.createUser({
      user: "admin",
      pwd: "secure_password",
      roles: [
        { role: "root", db: "admin" }
      ]
    });
    '
fi

# 관리자 계정으로 재인증
echo "Authenticating as admin user..."

# monstache 사용자가 존재하는지 확인 (인증 포함)
echo "Checking if monstache user exists..."
MONSTACHE_EXISTS=$(mongosh $AUTH_CONN --quiet --eval "db.getSiblingDB('admin').getUser('monstache') ? 1 : 0" 2>/dev/null || echo "0")

if [ "$MONSTACHE_EXISTS" == "1" ]; then
    echo "Monstache user already exists"
else
    echo "Creating monstache user..."
    mongosh $AUTH_CONN --eval '
    db = db.getSiblingDB("admin");
    db.createUser({
      user: "monstache",
      pwd: "secure_password",
      roles: [
        { role: "readAnyDatabase", db: "admin" },
        { role: "read", db: "local" },
        { role: "read", db: "config" },
        { role: "readWrite", db: "test" }
      ]
    });
    ' || {
      echo "Failed to create monstache user. Trying with explicit auth..."
      mongosh --host $MONGO_HOST --eval '
      db = db.getSiblingDB("admin");
      db.auth("admin", "secure_password");
      db.createUser({
        user: "monstache",
        pwd: "secure_password",
        roles: [
          { role: "readAnyDatabase", db: "admin" },
          { role: "read", db: "local" },
          { role: "read", db: "config" },
          { role: "readWrite", db: "test" }
        ]
      });
      '
    }
fi

echo "MongoDB setup completed"