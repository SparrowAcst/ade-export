const NodeCache = require("node-cache")
const uuid = require("uuid").v4
const { last, extend } = require("lodash")
const moment = require("moment")
const { Middlewares } = require('@molfar/amqp-client')
const { fileList } = require("./file-system")
const { getPublisher, getConsumer } = require("./export-messages")

const EXPORT_CACHE = new NodeCache({
    useClones: true
})

const REQUEST_CACHE = new NodeCache({
    useClones: false,
    stdTTL:  2 * 60 * 60,
    checkperiod: 5 * 60
})

let availableExports
let request

const processMessage = data => {
    console.log(data)
    const { requestId } = data
    let request
    if(REQUEST_CACHE.has(requestId)){
        request = REQUEST_CACHE.get(requestId)
    }

    if(request){
        request.log.push({
            date: new Date(),
            message: data.message
        })
    }

}


const getDatestamp = () => moment().format("YYYY-MM-DD_HH_mm_ss")

const init = async () => {
    
    if(!availableExports) {
        availableExports = await fileList(`./exports/*.js`)
        availableExports = availableExports.map(e => require(e))
        availableExports.forEach(r => {
            r.url = {
                dump: `./${r.alias}/dump`,
                diff: `./${r.alias}/diff`
            }
            EXPORT_CACHE.set(r.alias, r)
        })

        availableExports = {
                set: EXPORT_CACHE.set,
                get: EXPORT_CACHE.get,
                has: EXPORT_CACHE.has,
                keys: EXPORT_CACHE.keys,
                list: () => EXPORT_CACHE.keys().map(key => EXPORT_CACHE.get(key))
        }
    }

    if(!request){
        let publisher = await getPublisher()
        let consumer = await getConsumer()
        await consumer
            .use(Middlewares.Json.parse)
            .use((error, message, next) => {
                processMessage(message.content)
                next()
            })
            .use((err, msg, next) => {
                msg.ack()
                next()
            })
            .start()
        
        request = {
            create: options => {
                
                const datestamp = getDatestamp()
                
                options = extend({}, options, {
                    requestId: uuid(),
                    file: `${options.alias}-${datestamp}.${options.mode}.json`,
                    datestamp
                })

                let newRequest = {
                    options,  
                    log: [{
                        date: new Date(),
                        message: {
                            status: "pending"
                        }
                    }]
                }

                REQUEST_CACHE.set(options.requestId, newRequest)
                publisher.send(options)
                return newRequest
            },

            getRequest: requestId => {
                if(REQUEST_CACHE.has(requestId)){
                    return REQUEST_CACHE.get(requestId)
                }
            }, 
            getState: requestId => {
                if(REQUEST_CACHE.has(requestId)){
                    return last(REQUEST_CACHE.get(requestId).log)
                }
            },
            getLog: requestId => {
                if(REQUEST_CACHE.has(requestId)){
                    return REQUEST_CACHE.get(requestId).log
                }
            },
        }    

    }


    return {
        availableExports,
        request
    }

}





module.exports = init

