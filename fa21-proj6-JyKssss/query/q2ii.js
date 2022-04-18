// Task 2ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    {
        $project: {
                split : {
                    $split: [ "$tagline", " " ]
                }
        }
     },
    {$unwind : "$split"},
    {
        $project: {
            trim_split: { $trim: { input: "$split" , chars: ".,!?" } }
        }
    },
    {
        $project: {
            lower_split: { $toLower: "$trim_split" }
        }
    },
    {
        $project: {
            lower_split: 1,
            len : {$strLenCP: "$lower_split" }
        }
    },
    {$match: {len: { $gt:  3}}},
    {
        $group: {
            _id: "$lower_split",
            count: { $sum : 1}
        }
    },
    {$sort: {count : -1}},
    {$limit : 20}
]);