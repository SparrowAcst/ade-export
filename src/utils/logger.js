const { exists, mkdir, unlink, fileList, makeDirIfNotExists } = require("./file-system")
const { take, sortBy, isString } = require("lodash")
const winston = require("winston")
const path = require("path")
const moment = require("moment")

const WORK_DIR = "../EXPORT"
const config = require("./config")(path.resolve(`${WORK_DIR}/config.json`))

const format = winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.printf(info => `${info.timestamp} ${info.level.toUpperCase()} ` +  ((isString(info.message)) ? info.message : JSON.stringify(info.message, null, " ")) + ` ${info.stack || ""}`)
)

const errorsFormat = winston.format.errors({ stack: true })

let logger

const init = async config => {

    if (!logger) {

        const LOG_DIR = path.resolve(config.get("log.dir"))
        await makeDirIfNotExists(LOG_DIR)

        let logFiles = await fileList(`${config.get("log.dir")}/*.log`)
        logFiles = sortBy(logFiles)
        let toDelete = take(logFiles, logFiles.length - config.get("log.files") - 1)

        for(const file of toDelete){
            await unlink(file)
        }

        const APP_LOG = path.resolve(`${LOG_DIR}/${config.get("dateStamp")}.log`)



        logger = winston.createLogger({
            format,
            transports: [
                new winston.transports.File({ filename: APP_LOG }),
                // new winston.transports.Console({}),
            ],
        })
    }

    return logger
}


module.exports = init