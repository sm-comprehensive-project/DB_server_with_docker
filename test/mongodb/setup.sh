#!/bin/bash

# 복제 세트 초기화 전 충분한 대기 시간 제공
echo "Waiting for MongoDB instances to start..."
sleep 30

# MongoDB 복제 세트 초기화 시도
echo "Attempting to initialize replica set..."
mongosh mongodb://mongo1:27017 replicaSet.js

# 초기화 결과 확인
if [ $? -eq 0 ]; then
    echo "Replica set initialized successfully"
else
    echo "Failed to initialize replica set"
    exit 1
fi