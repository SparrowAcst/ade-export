const exportDataset = require("./utils/export-dataset")

const run = async () => {
	await exportDataset(require("../datasets/test_multi_device_v3_4"))		
}

run()

