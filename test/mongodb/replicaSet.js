config = {
  _id: "replication",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 1 }, 
    { _id: 1, host: "mongo2:27017", priority: 0 }, 
    { _id: 2, host: "mongo3:27017", priority: 0 }  
  ]
}

rs.initiate(config)
rs.conf()