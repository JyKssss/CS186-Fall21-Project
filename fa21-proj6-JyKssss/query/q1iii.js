// Task 1iii

db.ratings.aggregate([
    // TODO: Write your query here
    {
        $group: {
            _id: "$rating", // Group by the field rating
            count: {$sum: 1} // Get the count for each group
        }
     },
    {
        $project: {
                _id: 0, // explicitly project out this field
                rating: "$_id", // grab the title of first movie
                count: 1
        }
     },
    {$sort: {rating: -1}}
]);