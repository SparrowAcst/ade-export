
const {parser} = require('stream-json/jsonl/Parser')
const fs = require("fs")


const load = async file => new Promise((resolve, reject) => {
    
    let result =[]

    const fileStream = fs.createReadStream(file)
    const jsonStream = parser()
    fileStream.pipe(jsonStream);
    
    jsonStream.on('data', ({key, value}) => {
       result.push(value)
    });

    jsonStream.on('end', () => {
        resolve(result)
    })

    jsonStream.on('error', () => {
        reject()
    })


})

const getStream = file => {
    const fileStream = fs.createReadStream(file)
    const jsonStream = parser()
    jsonStream.pause()
    fileStream.pipe(jsonStream);
    
    return jsonStream
} 

module.exports = {
    load,
    getStream
} 

