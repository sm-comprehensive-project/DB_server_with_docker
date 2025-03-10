// 관리자 권한 사용자 생성
db.createUser({
    user: "admin",
    pwd: "qwer1234!",
    roles: [
      { role: "root", db: "admin" }
    ]
  });