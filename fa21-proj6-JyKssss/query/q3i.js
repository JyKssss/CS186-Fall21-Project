// Task 3i

db.credits.aggregate([
    // TODO: Write your query here
    { $unwind : "$cast"},
    { $match: {"cast.id": 7624}},
    {
         $lookup: {
            from: "movies_metadata",
            localField: "movieId",
            foreignField: "movieId",
            as: "meta"
        }
     },
    {
        $project: {
                _id: 0, // explicitly project out this field
                title: "$meta.title", // grab the title  movie
                release_date: "$meta.release_date", // rename release_date
                character : "$cast.character"
        }
     },
    { $unwind : "$title"},
    { $unwind : "$release_date"},
    { $sort: { release_date : -1}}

]);