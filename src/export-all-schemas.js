const exportDataset = require("./utils/export-dataset")

const run = async () => {
	await exportDataset(require("../exports/all-schemas"))		
}

run()

