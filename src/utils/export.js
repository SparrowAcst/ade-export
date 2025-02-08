
const fs = require("fs")
const {exists, makeDirIfNotExists, unlink} = require("./file-system")

const path = require("path")

const { isFunction, extend, find, keys, isUndefined } = require("lodash") 

const moment = require("moment")

const JSONL = require("./jsonl")

const docdb = require("./docdb")("TEST")

const WORK_DIR = "../EXPORT"
const config = require("./config")(path.resolve(`${WORK_DIR}/config.json`))

let CURRENT_DIR
let PREVIUS_DIR
let RESULT_DIR

let logger

const Diff = require("./diff")

const detectChanges = async (doc, pool) => {

    let prev = find( pool, d => d.id == doc.id)
    if(!prev) return true
    let delta = await Diff.delta(doc, prev)

    return keys(delta).map(key => !isUndefined(delta[key])).reduce((a, b) => a || b, false)

}


const processCollection = async options => {

    try {
    
      let { schema, collection, pipeline, mapper, outputFile } = options

      let datapool = [] 
      
      let poolFilePath = path.resolve(`${PREVIUS_DIR}/${outputFile}.${schema}.${collection}.jsonl`)
      const existsPoolFile = await exists(poolFilePath)

      if(existsPoolFile){
        datapool = await JSONL.load(poolFilePath)  
      }

      let filePath = path.resolve(`${CURRENT_DIR}/${outputFile}.${schema}.${collection}.jsonl`)
      let outputStream = fs.createWriteStream(filePath)
      
      let diffFilePath = path.resolve(`${CURRENT_DIR}/${outputFile}.${schema}.${collection}.diff.jsonl`)
      let diffOutputStream = fs.createWriteStream(diffFilePath)
      
      mapper = mapper || (d => d)
      mapper = (isFunction(mapper)) ? mapper : (d => d)

      const aggCursor = await docdb.getAggregateCursor({
        collection: `${schema}.${collection}`,
        pipeline
      })
      
      const source = aggCursor.cursor
      
      let counter = 0

      for await (const doc of source) {
        counter++
        process.stdout.write(`${schema}.${collection} > Export: ${counter} items                 ${'\x1b[0G'}`)
        let res = await mapper(doc)
        outputStream.write(JSON.stringify(res))
        outputStream.write("\n")
        const hasChanges = await detectChanges(res, datapool)
        if(hasChanges){
          logger.info(`Detect changes: ${schema}.${collection}.${doc.id}`)
          diffOutputStream.write(JSON.stringify(res))
          diffOutputStream.write("\n")
        }

      }

      await source.close()
      aggCursor.client.close()
      outputStream.end()
      diffOutputStream.end()
        
      logger.info(`\n${filePath} Done`)
      
    } catch (e) {
    
      throw e
    
    }  
}


const processDataset = async options => {

  try {

    const { commands, outputFile } = options

    logger = await require("./logger")(config)
    logger.info("Start ADE export")
    logger.info(config)

    CURRENT_DIR = path.resolve(config.get("currentVersion.dir"))
    PREVIUS_DIR = path.resolve(config.get("previusVersion.dir"))
    RESULT_DIR = path.resolve(config.get("result.dir"))

    await makeDirIfNotExists(CURRENT_DIR)
    await makeDirIfNotExists(PREVIUS_DIR)
    await makeDirIfNotExists(RESULT_DIR)
    
    for(let command of commands){
      
      command.outputFile = outputFile
      
      command.mapper = command.mapper || (d => {
        d.schema = command.schema
        d.collection = command.collection
        d.exportedAt = new Date()
        return d
      })

      await processCollection(command)
    
    }
    
    config.extend({
      previusVersion: config.get("currentVersion"),
      currentVersion: config.get("previusVersion"),
      lastUpdate: new Date()
    })

    config.save()
 
  } catch(e) {

    throw e

  }

}

module.exports = processDataset
