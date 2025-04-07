#!/bin/bash

# MongoDB 인스턴스가 시작될 때까지 대기
echo "Waiting for MongoDB instances to start..."
sleep 30

# 레플리카셋 상태 확인
echo "Checking replica set status..."
RS_STATUS=$(mongosh --host mongo1:27017 --quiet --eval "try { rs.status().ok } catch { 0 }")

if [ "$RS_STATUS" == "1" ]; then
    echo "Replica set is already initialized"
else
    echo "Initializing replica set..."
    mongosh --host mongo1:27017 /usr/src/scripts/replicaSet.js
fi

# 관리자 계정이 존재하는지 확인
echo "Checking if admin user exists..."
ADMIN_EXISTS=$(mongosh --host mongo1:27017 --quiet --eval "db.getSiblingDB('admin').getUser('admin') ? 1 : 0")

if [ "$ADMIN_EXISTS" == "1" ]; then
    echo "Admin user already exists"
else
    echo "Creating admin user..."
    mongosh --host mongo1:27017 --eval '
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

# monstache 사용자가 존재하는지 확인
echo "Checking if monstache user exists..."
MONSTACHE_EXISTS=$(mongosh --host mongo1:27017 --quiet --eval "db.getSiblingDB('admin').getUser('monstache') ? 1 : 0")

if [ "$MONSTACHE_EXISTS" == "1" ]; then
    echo "Monstache user already exists"
else
    echo "Creating monstache user..."
    mongosh --host mongo1:27017 --eval '
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
fi

echo "MongoDB setup completed"