const { get, set, extend } = require("lodash")
const fs = require("fs")
const moment = require("moment")

const Config = class {
    
    constructor(configFile){
        this.configFile = configFile
        this.content = require(configFile)
        this.content.dateStamp = moment().format("YYYY-MM-DD_HH_mm_ss")
    }

    get(key){
        return get(this.content, key)
    }

    set(key, value){
        set(this.content, key, value)
    }

    extend(update){
        this.content = extend( {}, this.content, update)
    }

    save(){
        fs.writeFileSync(this.configFile, JSON.stringify(this.content, null, " "))
    }

}

module.exports = configFile => new Config(configFile)