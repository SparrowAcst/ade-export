const { extend } = require("lodash")
const { AmqpManager, Middlewares } = require('@molfar/amqp-client');

const processDataset = require("./utils/export-2")

const config = require("../.config-migrate-db")
const configRB = config.rabbitmq
const normalize = configRB.normalize

const PUBLISHER_OPTIONS = normalize({
    exchange: {
        name: `export_log_exchange`,
        options: {
            durable: true,
            persistent: true
        }
    }
})

let PUBLISHER

const CONSUMER_OPTIONS = normalize({
    queue: {
        name: "export_request",
        exchange: {
            name: 'export_request_exchange',
            options: {
                durable: true,
                persistent: true
            }
        },
        options: {
            noAck: false,
            exclusive: false
        }
    }

})


const getPublisher = async () => {
    if(!PUBLISHER){
        PUBLISHER = await AmqpManager.createPublisher(PUBLISHER_OPTIONS)
        PUBLISHER.use(Middlewares.Json.stringify)
    }
    return PUBLISHER
}


const processData = async (err, data, next) => {
    
    try {
        const publisher = await getPublisher()
        await processDataset(data.content, publisher)
        next()

    } catch (e) {
        console.log(e.toString(), e.stack)
        throw e
    }
}



const run = async () => {

    console.log(`Configure ADE Export Microservice `)
    console.log("Consumer:", CONSUMER_OPTIONS)
    console.log("Publisher:", PUBLISHER_OPTIONS)
    
    const consumer = await AmqpManager.createConsumer(CONSUMER_OPTIONS)

    await consumer
        .use(Middlewares.Json.parse)

        .use(async (err, msg, next) => {
            const publisher = await getPublisher()

            publisher.send({
                requestId: msg.content.requestId,
                message:{
                    date: new Date(),
                    status: "started",
                    message: `Start in "${msg.content.mode}" mode`    
                }
                
            })
            console.log(msg.content.requestId, `Start in "${msg.content.mode}" mode`)
            next()
        })

        .use(processData)

        .use(Middlewares.Error.Log)
        .use(Middlewares.Error.BreakChain)

        .use((err, msg, next) => {
            msg.ack()
            next()
        })

        .start()

    console.log(`ADE Export Microservice started`)
    
}

run()