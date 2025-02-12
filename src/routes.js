const router = require('express').Router()
const initExportManager = require("./utils/export-manager")
const { extend } = require("lodash")


/**
 * @openapi
 * /:
 *   get:
 *     description: Retrieve a list of available exports
 *     responses:
 *       200:
 *         description: A list of available exports
 */

router.get('/', async (req, res) => {
    const exportManager = await initExportManager()
    res.send(exportManager.availableExports.list())
})


/**
 * @openapi
 * /export/{alias}/{mode}:
 *   get:
 *     description: Retrieve a list of available exports
 *     parameters:
 *           - name: "alias"
 *             in: "path"
 *             description: "Alias of export"
 *             required: true
 *             type: "string"
 *           - name: "mode"
 *             in: "path"
 *             description: "Export mode"
 *             required: false
 *             type: "string"
 *             enum:
 *               - diff
 *               - dump
 *             default: diff  
 *   
 *     responses:
 *       200:
 *         description: A iinfo of export request
 *         schema:
 *           type: "object"
 *           required:
 *               - "requiestId"
 *               - "file"
 *           properties:
 *             requestId:
 *               type: "string"
 *               format: "uuid"
 *             file:
 *               type: "string"
 *       "503":
 *         description: "Internal server error"
 *       "404":
 *         description: "Export not found"
 */

router.get('/export/:alias/:mode', async (req, res) => {
    let { alias, mode } = req.params
    mode = mode || "diff"    
    mode = (["dump", "diff"].includes(mode)) ? mode : "diff"
    const exportManager = await initExportManager()
    if(exportManager.availableExports.has(alias)){
        let options = extend({}, exportManager.availableExports.get(alias), { mode })
        let request = exportManager.request.create(options)
        res.send(request)
    } else {
        res.status(404).send(`Export "${alias}" not found`)
    }

})


/**
 * @openapi
 * /status/{requestId}:
 *   get:
 *     description: Retrieve a request status
 *     parameters:
 *           - name: "requestId"
 *             in: "path"
 *             description: "Request identifier"
 *             required: true
 *             type: "string"
 *             format: "uuid" 
 *   
 *     responses:
 *       200:
 *         description: A request status
 *       "503":
 *         description: "Internal server error"
 *       "404":
 *         description: "Request not found"
 */

router.get('/status/:requestId', async (req, res) => {
    
    let { requestId } = req.params
    const exportManager = await initExportManager()
    let data = exportManager.request.getState(requestId)
    
    if(data){
        res.send(data)
    } else {
        res.status(404).send(`Request "${requestId}" not found`)
    }

})


/**
 * @openapi
 * /log/{requestId}:
 *   get:
 *     description: Retrieve a request log
 *     parameters:
 *           - name: "requestId"
 *             in: "path"
 *             description: "Request identifier"
 *             required: true
 *             type: "string"
 *             format: "uuid" 
 *   
 *     responses:
 *       200:
 *         description: A request log
 *       "503":
 *         description: "Internal server error"
 *       "404":
 *         description: "Request not found"
 */

router.get('/log/:requestId', async (req, res) => {
    
    let { requestId } = req.params
    const exportManager = await initExportManager()
    let data = exportManager.request.getLog(requestId)
    
    if(data){
        res.send(data)
    } else {
        res.status(404).send(`Request "${requestId}" not found`)
    }

})



module.exports = router