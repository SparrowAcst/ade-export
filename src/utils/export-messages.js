const uuid = require("uuid").v4
const { extend } = require("lodash")
const { AmqpManager, Middlewares } = require('@molfar/amqp-client')

const config = require("../../.config-migrate-db")
const configRB = config.rabbitmq
const normalize = configRB.normalize



const PUBLISHER_OPTIONS = normalize({
    exchange: {
        name: `export_request_exchange`,
        options: {
            durable: true,
            persistent: true
        }
    }
})

console.log(PUBLISHER_OPTIONS)
let PUBLISHER

const CONSUMER_OPTIONS = normalize({
    queue: {
        name: "export_log",
        exchange: {
            name: 'export_log_exchange',
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
console.log(CONSUMER_OPTIONS)


let CONSUMER

const getPublisher = async () => {
    if (!PUBLISHER) {

        PUBLISHER = await AmqpManager.createPublisher(PUBLISHER_OPTIONS)
        PUBLISHER.use(Middlewares.Json.stringify)
    }
    return PUBLISHER
}

const getConsumer = async () => {
    if (!CONSUMER) {
        CONSUMER = await AmqpManager.createConsumer(CONSUMER_OPTIONS)
        // await CONSUMER
        //     .use(Middlewares.Json.parse)

        //     .use((err, msg, next) => {
        //         msg.ack()
        //         next()
        //     })

        //     // .use((err, msg, next)=> {
        //     //     console.log("Employee Manager receive:", msg.content)
        //     // })

        //     .start()

    }
    return CONSUMER
}


module.exports = {
    getPublisher,
    getConsumer
}