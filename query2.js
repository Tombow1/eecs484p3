// Query 2
// Unwind friends and create a collection called 'flat_users' where each document has the following schema:
// {
//   user_id:xxx
//   friends:xxx
// }
// Return nothing.

function unwind_friends(dbname) {
    db = db.getSiblingDB(dbname);

    // TODO: unwind friends
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
    return;
}
