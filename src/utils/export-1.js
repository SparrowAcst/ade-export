
const fs = require("fs")
const path = require("path")
const { isFunction, extend, find, keys, isUndefined, findIndex } = require("lodash") 
const moment = require("moment")
const merge = require("merge-files")

const {exists, makeDirIfNotExists, unlink} = require("./file-system")
const { uploadFromStream } = require("./s3-bucket")

const JSONL = require("./jsonl")

const docdb = require("./docdb")("TEST")

// const WORK_DIR = "../EXPORT"
// const config = require("./config")(path.resolve(`${WORK_DIR}/config.json`))

let CURRENT_DIR
let PREVIUS_DIR
let RESULT_DIR
let CONFIG
let logger

const Diff = require("./diff")

const detectChanges = async (doc, pool) => {

    let prev = find( pool, d => d.id == doc.id)
    if(!prev) return true
    let delta = await Diff.delta(doc, prev)

    return keys(delta).map(key => !isUndefined(delta[key])).reduce((a, b) => a || b, false)

}

const writeBuffer = (stream, buffer) => {

  for(const doc of buffer){
    stream.write(JSON.stringify(doc))
    stream.write("\n")
  }

}

const getDataPoll = async ({schema, collection, alias, buffer}) => new Promise( async (resolve, reject) => {

    let result = []
    
    let poolFilePath = path.resolve(`${PREVIUS_DIR}/${alias}.${schema}.${collection}.json`)
    const existsPoolFile = await exists(poolFilePath)
    if(!existsPoolFile) {
      resolve(result)
      return
    }  

    const poolStream = JSONL.getStream(poolFilePath)
    
    poolStream.on('data', ({key, value}) => {
       let f = find(buffer, b => b.id == value.id)
       if(f) result.push(value)
    })

    poolStream.on('end', () => {
        resolve(result)
    })

    poolStream.on('error', () => {
        reject()
    })

    poolStream.resume()

})


const findChanges = async (buffer, exclusions, options) => {
    
    let { schema, collection, pipeline, mapper, alias } = options
    let datapool = await getDataPoll(extend({}, options, { buffer }))
    let result = []
    for(const doc of buffer){
      const hasChanges = await detectChanges(doc, datapool)
      if(hasChanges) {
        result.push(doc)
      }  
    } 

    result = result.filter( r => !exclusions.includes(r.id))
    result.forEach( r => {
      logger.info(`Find changes in ${alias}.${schema}.${collection}.${r.id}`)
    })
    return result

}


const processCollection = async options => {

    try {
    
      let { schema, collection, pipeline, mapper, alias } = options
      logger.info(`Process ${alias}.${schema}.${collection}`)
      const BUFFER_SIZE = CONFIG.get("bufferSize")

      let filePath = path.resolve(`${CURRENT_DIR}/${alias}.${schema}.${collection}.json`)
      let outputStream = fs.createWriteStream(filePath)
      
      let diffFilePath = path.resolve(`${CURRENT_DIR}/${alias}.${schema}.${collection}.diff.json`)
      let diffOutputStream = fs.createWriteStream(diffFilePath)
      
      mapper = mapper || (d => d)
      mapper = (isFunction(mapper)) ? mapper : (d => d)

      const aggCursor = await docdb.getAggregateCursor({
        collection: `${schema}.${collection}`,
        pipeline
      })
      
      const source = aggCursor.cursor
         
      let counter = 0
      let buffer = []
      let exclusions = []
      for await (const doc of source) {
        counter++
        process.stdout.write(`${schema}.${collection} > Export: ${counter} items                 ${'\x1b[0G'}`)
        let res = await mapper(doc)
        buffer.push(res)
 
        if(buffer.length >= BUFFER_SIZE){
          writeBuffer(outputStream, buffer)
          let changes = await findChanges(buffer, exclusions, options)
          writeBuffer(diffOutputStream, changes)
          exclusions = exclusions.concat(buffer.map( b => b.id))
          buffer = []
        }
      }

      if(buffer.length > 0){

        writeBuffer(outputStream, buffer)
        let changes = await findChanges(buffer, exclusions, options)
        writeBuffer(diffOutputStream, changes)
  
      }

      await source.close()
      aggCursor.client.close()
      outputStream.end()
      diffOutputStream.end()
        
      logger.info(`${alias}.${schema}.${collection} processed.`)
      
    } catch (e) {
    
      throw e
    
    }  
}


const mergeFiles = async options => {
  
  let target = path.resolve(`${RESULT_DIR}/${options.outputFileName}`)
  let sources = options.commands.map( c => path.resolve(`${CURRENT_DIR}/${options.alias}.${c.schema}.${c.collection}.diff.json`))
  logger.info(`Merge files:`)
  logger.info(sources)
  logger.info(`Target: ${target}`)
  await merge(sources, target)
      
}


const upload = async options => {
  
  const source = path.resolve(`${RESULT_DIR}/${options.outputFileName}`)
  const stream = fs.createReadStream(source)
  const target = `ADE-EXPORTS/${options.outputFileName}`
  const callback = event => {
    process.stdout.write(`Uploaded :: ${event.loaded} from ${event.total} ${(event.loaded * 100 / event.total).toFixed(2)}%                 ${'\x1b[0G'}`)
  }

  await uploadFromStream({ 
    stream, 
    target, 
    callback 
  })

  logger.info(`Upload ${source} to ${target}`)

}

const processDataset = async (config, options) => {

  try {

    CONFIG = config

    const { commands, alias } = options

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
      
      command.alias = alias
      
      command.mapper = command.mapper || (d => {
        d.schema = command.schema
        d.collection = command.collection
        d.exportedAt = new Date()
        return d
      })

      await processCollection(command)
    }

    await mergeFiles(options)
    await upload(options)
    
    config.extend({
      previusVersion: config.get("currentVersion"),
      currentVersion: config.get("previusVersion"),
    })

    config.save()
 
  } catch(e) {

    throw e

  }

}

module.exports = processDataset
