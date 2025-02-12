
const fs = require("fs")
const path = require("path")
const { isFunction, extend, find, keys, isUndefined, findIndex } = require("lodash") 
const moment = require("moment")
const merge = require("merge-files")

const {exists, makeDirIfNotExists, unlink, fileList} = require("./file-system")
const { uploadFromStream } = require("./s3-bucket")

const JSONL = require("./jsonl")

const docdb = require("./docdb")("ADE")


const POOL_DIR = "../EXPORT/POOL"
const RESULT_DIR = "../EXPORT/RESULT"
const BUFFER_SIZE = 2

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

const getDataPoll = async ({schema, collection, alias, buffer, previusDump}) => new Promise( async (resolve, reject) => {

    let result = []
    
    let poolFilePath = previusDump
    const existsPoolFile = await exists(poolFilePath)
    console.log(poolFilePath, existsPoolFile)
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


const findChanges = async (buffer, exclusions, command) => {
    
    let { schema, collection, pipeline, mapper, alias } = command
    let datapool = await getDataPoll(extend({}, command, { buffer }))
    let result = []
    for(const doc of buffer){
      const hasChanges = await detectChanges(doc, datapool)
      if(hasChanges) {
        result.push(doc)
      }  
    } 

    result = result.filter( r => !exclusions.includes(r.id))
    result.forEach( r => {
      // logger.info(`Find changes in ${alias}.${schema}.${collection}.${r.id}`)
    })
    return result

}


const processDIff = async (command, datestamp) => {

    try {
    
      let { schema, collection, pipeline, mapper, alias, previusDump } = command
      
      let filePath = path.resolve(`${POOL_DIR}/${alias}.${schema}.${collection}-${datestamp}.dump.json`)
      let outputStream = fs.createWriteStream(filePath)
      
      let diffFilePath = path.resolve(`${POOL_DIR}/${alias}.${schema}.${collection}-${datestamp}.diff.json`)
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
          let changes = await findChanges(buffer, exclusions, command)
          writeBuffer(diffOutputStream, changes)
          exclusions = exclusions.concat(buffer.map( b => b.id))
          buffer = []
        }
      }

      if(buffer.length > 0){

        writeBuffer(outputStream, buffer)
        let changes = await findChanges(buffer, exclusions, command)
        writeBuffer(diffOutputStream, changes)
  
      }

      await source.close()
      aggCursor.client.close()
      outputStream.end()
      diffOutputStream.end()
        
    } catch (e) {
    
      throw e
    
    }  
}

const processDump = async (command, datestamp) => {

    try {
    
      let { schema, collection, pipeline, mapper, alias } = command
      
      let filePath = path.resolve(`${POOL_DIR}/${alias}.${schema}.${collection}-${datestamp}.dump.json`)
      let outputStream = fs.createWriteStream(filePath)
      
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
          buffer = []
        }
      }

      if(buffer.length > 0){
        writeBuffer(outputStream, buffer)
      }

      await source.close()
      aggCursor.client.close()
      outputStream.end()
        
      
    } catch (e) {
    
      throw e
    
    }  
}


const findPreviusDump =  async ({alias, schema, collection}) => {
  let f = await fileList(`${POOL_DIR}/${alias}.${schema}.${collection}-*.dump.json`)  
  return f[0]
}

const findPreviusDiff =  async ({alias, schema, collection}) => {
  let f = await fileList(`${POOL_DIR}/${alias}.${schema}.${collection}-*.diff.json`)  
  return f[0]
}


const processCollection = async (command, datestamp, mode) => {

    try {
    
      let { schema, collection, pipeline, mapper, alias } = command
      
      command.previusDump = await findPreviusDump( command)
      command.previusDiff = await findPreviusDiff( command)

      if(mode == "diff") {
        await processDIff(command, datestamp)
      } else {
        await processDump(command, datestamp)
      }

      if(command.previusDump) await unlink(command.previusDump)
      if(command.previusDiff) await unlink(command.previusDiff)


    } catch (e) {
    
      throw e
    
    }  
}


const mergeFiles = async settings => {
  
  let target = path.resolve(`${RESULT_DIR}/${settings.file}`)
  let sources = settings.commands.map( c => path.resolve(`${POOL_DIR}/${settings.alias}.${c.schema}.${c.collection}-${settings.datestamp}.${settings.mode}.json`))
  console.log(sources)
  let existed = []
  for(const s of sources){
    const ex = await exists(s)
    if(ex){
      existed.push(s)
    }else{
      console.log(`${s} not found`)
    }
  }
  await merge(existed, target)
      
}


const upload = async settings => {
  
  const source = path.resolve(`${RESULT_DIR}/${settings.file}`)
  const stream = fs.createReadStream(source)
  const target = `ADE-EXPORTS/${settings.file}`
  const callback = event => {
    process.stdout.write(`Uploaded :: ${event.loaded} from ${event.total} ${(event.loaded * 100 / event.total).toFixed(2)}%                 ${'\x1b[0G'}`)
  }

  await uploadFromStream({ 
    stream, 
    target, 
    callback 
  })

}


const processDataset = async (settings, publisher) => {

  try {
   
    const { requestId, mode, file, datestamp, commands, alias} = settings 
    
    await makeDirIfNotExists(POOL_DIR)
    await makeDirIfNotExists(RESULT_DIR)
    
    publisher.send({
      requestId,
      message: {
        date: new Date(),
        status: "pre-process"  
      }
    })

    console.log(requestId, "pre-process")

    let index = 0
    for(let command of commands){
         
      index++
      
      command.alias = alias
      
      command.mapper = command.mapper || (d => {
        d.schema = command.schema
        d.collection = command.collection
        d.exportedAt = new Date()
        return d
      })

      publisher.send({
        requestId,
        message: {
          date: new Date(),
          status: "process",
          message: `process ${index} from ${commands.length}: ${command.schema}.${command.collection}`  
        }
      })
      console.log(requestId, "process", `Process ${index} from ${commands.length}: ${command.schema}.${command.collection}`)

      await processCollection(command, datestamp, mode)
    }

    publisher.send({
        requestId,
        message: {
          date: new Date(),
          status: "post-process",
          message: `Merge files into ${settings.file}`  
        }
      })
    console.log(requestId, "post-process", `Merge files into ${settings.file}`)

    await mergeFiles(settings)
    
    console.log("!!!!")

    publisher.send({
        requestId,
        message: {
          date: new Date(),
          status: "post-process",
          message: `Upload ${settings.file}`  
        }
      })
    console.log(requestId, "post-process", `Upload ${settings.file}`)
    
    await upload(settings)

    await unlink(path.resolve(`${RESULT_DIR}/${settings.file}`))

    publisher.send({
        requestId,
        message: {
          date: new Date(),
          status: "done"  
        }
      })
    console.log(requestId, "done")
    
  } catch(e) {

    throw e

  }

}

module.exports = processDataset
