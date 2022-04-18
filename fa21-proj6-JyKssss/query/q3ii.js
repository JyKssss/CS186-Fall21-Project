// Task 3ii

db.credits.aggregate([
    // TODO: Write your query here
    {
        $match : {
            crew : { $elemMatch: { id: 5655, job : "Director" }}
        }
    },
    { $unwind : "$cast"},
    {
        $project: {
            id: "$cast.id",
            name : "$cast.name",
            _id : 0
        }
    },
    {
        $group: {
            _id: {name : "$name" , id : "$id"}, // Group by the field movieId
            count: {$sum: 1} // Get the count for each group
        }
     },
     {
        $project: {
            id: "$_id.id",
            name : "$_id.name",
            count : 1,
            _id : 0
        }
    },
    { $sort: { count : -1, id : 1}},
    {$limit : 5}
]);