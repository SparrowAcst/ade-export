const collection = "labels"

const pipeline = [{
        $match: {
            id: {
                $ne: null
            },
            // state: "Finalized",
            "Body Spot": {
                $in: [
                    "Tricuspid",
                    "Erb's",
                    "Aortic",
                    "Right carotid",
                    "Pulmonic",
                    "Right Carotid",
                    "Erb's Right",
                    "Apex",
                ],
            },
            segmentation: {
                $exists: true,
            },
            // "Do not use on heart sound models": {
            //     $exists: false,
            // },
            // "Use cautiously with heart sound models": {
            //     $exists: false,
            // },
            // "Do not use on lung models": {
            //     $exists: false,
            // },
            // "Use cautiously with lung sound models": {
            //     $exists: false,
            // },
        },
    },
    {
        $project: {
            _id: 0,
            "Type of artifacts , Artifact": 0,
            complete: 0,
            Confidence: 0,
            taskList: 0

        }
    }
]

module.exports = {
    alias: "test-dataset",
    commands: [{
            schema: "strazhesko-part-1",
            collection,
            pipeline
        },
        {
            schema: "poltava-part-1",
            collection,
            pipeline
        },
        {
            schema: "digiscope",
            collection,
            pipeline
        }
    ]
}