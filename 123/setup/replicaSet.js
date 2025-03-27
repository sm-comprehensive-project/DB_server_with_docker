var rs = new ReplSetTest({
  name: "replication",
  nodes: 2
});

var config = {
  _id: "replication",
  members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" }
  ]
};

rs.initiate(config);
rs.awaitReplication();
rs.printStatus();