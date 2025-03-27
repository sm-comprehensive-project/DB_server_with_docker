// 관리자 데이터베이스에 루트 사용자 생성
db = db.getSiblingDB('admin');
db.createUser({
    user: "admin",
    pwd: "qwer1234!",
    roles: [
        { role: "userAdminAnyDatabase", db: "admin" },
        { role: "clusterAdmin", db: "admin" },
        { role: "readWriteAnyDatabase", db: "admin" }
    ]
});

// damoa 데이터베이스에 일반 사용자 생성
db = db.getSiblingDB('damoa');
db.createUser({
    user: "damoa_user",
    pwd: "damoa_password",
    roles: [
        { role: "readWrite", db: "damoa" }
    ]
});