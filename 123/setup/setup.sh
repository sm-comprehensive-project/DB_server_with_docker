#!/bin/bash
echo "MongoDB 인스턴스 준비 대기 중..."
sleep 30

mongo mongodb://admin:qwer1234!@mongo1:27017/admin << EOF
rs.initiate({
    _id : "rp0",
    members: [
      {_id:0, host : "mongo1:27017"},
      {_id:1, host : "mongo2:27017"}
    ]
});
rs.status();

// 복제 세트 설정 후 인증 확인
use admin
db.auth("admin", "qwer1234!")
EOF