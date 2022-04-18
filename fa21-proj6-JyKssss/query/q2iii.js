// Task 2iii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    { $project : {
        revise_budget : {
            $cond: {
                if:{$and : [
                        { $ne: ["$budget", ""] },
                        { $ne: ["$budget", undefined] },
                        { $ne: ["$budget", false] },
                        { $ne: ["$budget", null] }
                    ] } ,
                then:{
                        $round: [{ $cond: {
                            if: { $isNumber: "$budget" }, then: "$budget", else: {$toInt: {$trim: {input: "$budget",chars: " USD\$"}}}}
                                }, -7]
                },
                else : "unknown"
            }
        }
        }},
    {
        $group: {
            _id: "$revise_budget",
            count: { $sum : 1}
        }
    },
    {
        $project: {
            budget : "$_id",
            count : 1,
            _id: 0
        }
    },
    { $sort: { budget : 1}}
]);