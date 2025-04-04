config = {
  _id: "replication",
  members: [
    { _id: 0, host: "test-mongo1-1:27017", priority: 3 }, 
    { _id: 1, host: "test-mongo2-1:27017", priority: 2 }, 
    { _id: 2, host: "test-mongo3-1:27017", priority: 1 }  
  ]
}

rs.initiate(config)
rs.conf()