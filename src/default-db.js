// Convenient helper to setup dynamodb connection using environment variables.
// The constructed db instance will be cached by NodeJS.

const AWS = require('aws-sdk')

const setup = require('./dynamodb')

const awsConfig = {
  region: 'us-west-2',
  endpoint: process.env.DYNAMO_ENDPT
}

const inDebugger = !!Number(process.env.INDEBUGGER)

const dynamoDBClient = new AWS.DynamoDB(awsConfig)
let dynamoDBDocumentClient
/* istanbul ignore if */
if (!inDebugger &&
    process.env.DAX_ENDPOINT) {
  awsConfig.endpoints = [process.env.DAX_ENDPOINT]
  const AwsDaxClient = require('amazon-dax-client')
  const daxDB = new AwsDaxClient(awsConfig)
  dynamoDBDocumentClient = new AWS.DynamoDB.DocumentClient({
    service: daxDB
  })
} else {
  dynamoDBDocumentClient = new AWS.DynamoDB.DocumentClient({
    service: dynamoDBClient
  })
}

module.exports = setup({
  dynamoDBClient,
  dynamoDBDocumentClient
})
