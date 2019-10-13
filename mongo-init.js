db.createUser(
        {
            user: "admin",
            pwd: "admin123456",
            roles: [
                {
                    role: "readWrite",
                    db: "Dota2"
                }
            ]
        }
);

db.createCollection(publicgames);
db.createCollection(rankedgames);
db.createCollection(professionalgames);
db.createCollection(matchresults);
