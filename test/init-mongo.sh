#!/bin/bash
chmod 400 /mongodb-keyfile
chown mongodb:mongodb /mongodb-keyfile
exec mongod --replSet replication --bind_ip_all --keyFile /mongodb-keyfile