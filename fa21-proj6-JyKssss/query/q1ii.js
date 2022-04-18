// Task 1ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    {$match: {genres: {$elemMatch: {name: "Comedy"}}}},
    {$match: {vote_count: { $gte:  50}}},
    {$sort: {vote_average: -1, vote_count: -1, movieId : 1}},
    {$limit: 50},
    {
        $project: {
                _id: 0, // explicitly project out this field
                title: 1, // grab the title of first movie
                vote_average: 1, // rename count to num_ratings
                vote_count: 1,
                movieId: 1
        }
     }
]);