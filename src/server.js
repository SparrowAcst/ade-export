const bodyParser = require('body-parser')
const express = require('express')
const morgan = require("morgan")
const path = require("path")
const swaggerUi = require('swagger-ui-express');
const swaggerJsDoc = require('swagger-jsdoc');

const app = express();
const port = 3000;

routes = require("./routes")

const swaggerOptions = {
  swaggerDefinition: {
    myapi: '3.0.0',
    info: {
      title: 'ADE Export API',
      version: '1.0.0',
      description: 'API documentation',
    },
    servers: [
      {
        url: 'http://localhost:3000',
      },
    ],
  },
  apis: ['./src/routes.js'],
};

app.use(morgan(':date[iso] :method :url :status :res[content-length] - :response-time ms'))
app.use(bodyParser.text());
app.use(bodyParser.urlencoded({
    parameterLimit: 100000,
    limit: '50mb',
    extended: true
}));
app.use(bodyParser.json({
    limit: '50mb'
}));

const swaggerDocs = swaggerJsDoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocs));

app.use(routes)

app.listen(port, () => {
    console.log(`ADE Export Service starts on port ${port}.`);
})