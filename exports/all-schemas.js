const collection = "labels"
const pipeline = [
    // {
    //     $match: {
    //         // state: "Finalized",
    //         "Body Spot": {
    //             $in: [
    //                 "Tricuspid",
    //                 "Erb's",
    //                 "Aortic",
    //                 "Right carotid",
    //                 "Pulmonic",
    //                 "Right Carotid",
    //                 "Erb's Right",
    //                 "Apex",
    //             ],
    //         }
    //     },
    // },
    // {
    //     $limit: 5
    // },
    {
        $project: {
            _id: 0,
            examinationId: 1,
            id: 1,
            // "Type of artifacts , Artifact": 0,
            // complete: 0,
            // Confidence: 0,
            // taskList: 0

        }
    }
]

const schemas = [
    "strazhesko-part-1",
    "strazhesko-part-2",
    "strazhesko-part-3",
    "poltava-part-1",
    "poltava-part-2",
    "poltava-part-3",
    "potashev-part-1",
    "potashev-part-2",
    "potashev-part-3",
    "denis-part-1",
    "denis-part-2",
    "denis-part-1",
    "digiscope",
    "arabia",
    "clinic4",
    "harvest1", 
    "hha",
    "hosphum-part-3",
    "innocent-reallife-ua",
    "innocent-reallife-us",
    "phisionet",
    "phonendo",
    "vinil",
    "vintage",
    "yoda"
]

module.exports = {
    alias: "all-schemas",
    description: "Export labels.id and labels.examinationId for all dataset schemas",
    commands: schemas.map(schema =>({
        schema,
        collection,
        pipeline
    }))

    // [{
    //         schema: "strazhesko-part-1",
    //         collection,
    //         pipeline
    //     },
    //     {
    //         schema: "poltava-part-1",
    //         collection,
    //         pipeline
    //     },
    //     {
    //         schema: "digiscope",
    //         collection,
    //         pipeline
    //     }
    // ]
}