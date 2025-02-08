const path = require("path")
const execute = require("./export-1")

const config = require("./config")(path.resolve(`./src/config.json`))

module.exports = async options => {
    options.outputFileName = `${options.alias}-${config.get("dateStamp")}.diff.json`
    await execute(config, options)
}

