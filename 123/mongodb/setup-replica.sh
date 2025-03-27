#!/bin/bash
sleep 30  # MongoDB 인스턴스가 완전히 시작될 때까지 대기

mongosh -u admin -p qwer1234! --authenticationDatabase admin <<EOF
rs.initiate({
  _id: "rp0",
  members: [
    { _id: 0, host: "mongo1:27017" },
    { _id: 1, host: "mongo2:27017" }
  ]
})
EOF