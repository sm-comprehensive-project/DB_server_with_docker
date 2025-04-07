config = {
  _id: "replication",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 3 }, 
    { _id: 1, host: "mongo2:27017", priority: 2 }, 
    { _id: 2, host: "mongo3:27017", priority: 1 }  
  ]
}

// 레플리카셋 초기화
rs.initiate(config);

// 초기화 확인 
rs.status();