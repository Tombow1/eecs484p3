// Query 5
// Find the oldest friend for each user who has a friend. For simplicity,
// use only year of birth to determine age, if there is a tie, use the
// one with smallest user_id. You may find query 2 and query 3 helpful.
// You can create selections if you want. Do not modify users collection.
// Return a javascript object : key is the user_id and the value is the oldest_friend id.
// You should return something like this (order does not matter):
// {user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname) {
    db = db.getSiblingDB(dbname);

    let results = {};
    // TODO: implement oldest friends
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


    let symmetricFriends = {};
    db.flat_users.find().forEach(user => {
        let user_id = user.user_id;
        let friend_id = user.friends;

        if (!symmetricFriends[user_id]) {
            symmetricFriends[user_id] = [];
        }
        if (!symmetricFriends[friend_id]) {
            symmetricFriends[friend_id] = [];
        }

        if (!symmetricFriends[user_id].includes(friend_id)) {
            symmetricFriends[user_id].push(friend_id);
        }
        if (!symmetricFriends[friend_id].includes(user_id)) {
            symmetricFriends[friend_id].push(user_id);
        }
    });

    for (let user_id in symmetricFriends) {
        let friends = Array.from(symmetricFriends[user_id]);
        if (friends.length > 0) {
            let oldest = db.users.find({ user_id: { $in: friends } },{ user_id: 1, YOB: 1, _id: 0 }).sort({ YOB: 1, user_id: 1 }).toArray();
            results[user_id] = oldest[0].user_id;
        }

    }
    return results;
}
