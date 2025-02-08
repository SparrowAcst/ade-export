const path = require("path")
const execute = require("./export-1")

const WORK_DIR = "../EXPORT"
const config = require("./config")(path.resolve(`${WORK_DIR}/config.json`))

module.exports = async options => {
    options.outputFileName = `${options.alias}-${config.get("dateStamp")}.diff.json`
    await execute(config, options)
}

