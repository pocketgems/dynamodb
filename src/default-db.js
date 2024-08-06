// Convenient helper to setup dynamodb connection using environment variables.
// The constructed db instance will be cached by NodeJS.

const { DynamoDB } = require('@aws-sdk/client-dynamodb')
const {
  DynamoDBDocument,
  DynamoDBDocumentClient
} = require('@aws-sdk/lib-dynamodb')

const setup = require('./dynamodb')

const awsConfig = {
  region: 'us-west-2',
  endpoint: process.env.DYNAMO_ENDPT
}

const inDebugger = !!Number(process.env.INDEBUGGER)

const dynamoDBClient = new DynamoDB(awsConfig)
// A DynamoDB Document Client instance without DAX integration
const documentClientWithoutDAX = DynamoDBDocument.from(dynamoDBClient, {
  marshallOptions: {
    removeUndefinedValues: true
  }
})
// This instance is conditionally configured to use the DAX client if the
// DAX endpoint is present and it is not in debug mode
let dynamoDBDocumentClient
/* istanbul ignore if */
if (!inDebugger &&
    process.env.DAX_ENDPOINT) {
  awsConfig.endpoints = [process.env.DAX_ENDPOINT]
  const AwsDaxClient = require('amazon-dax-client-sdkv3')
  const daxDB = new AwsDaxClient({
    client: DynamoDBDocumentClient.from(dynamoDBClient, {
      marshallOptions: {
        removeUndefinedValues: true
      }
    })
  })
  dynamoDBDocumentClient = daxDB
} else {
  dynamoDBDocumentClient = documentClientWithoutDAX
}

module.exports = setup({
  dynamoDBClient,
  dynamoDBDocumentClient,
  documentClientWithoutDAX
})
