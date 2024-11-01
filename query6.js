// Query 6
// Find the average friend count per user.
// Return a decimal value as the average user friend count of all users in the users collection.

function find_average_friendcount(dbname) {
    db = db.getSiblingDB(dbname);

    // TODO: calculate the average friend count
    db.users.aggregate([
        {
            $project: {
                user_id: 1,
                friends: 1,
                _id: 0
            }
        },
        {
            $unwind: "$friends"
        },
        {
            $out: "flat_users"
        }
    ]);
    userCount = db.users.count();

    totalFriendCount = db.flat_users.count();
    if (userCount > 0) {
        result = totalFriendCount / userCount;
    }
    return result;
}
