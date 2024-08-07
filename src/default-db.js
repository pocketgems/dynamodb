// Convenient helper to setup dynamodb connection using environment variables.
// The constructed db instance will be cached by NodeJS.

const { DynamoDB } = require('@aws-sdk/client-dynamodb')
const {
  DynamoDBDocument
} = require('@aws-sdk/lib-dynamodb')

const setup = require('./dynamodb')

const awsConfig = {
  region: 'us-west-2',
  endpoint: process.env.DYNAMO_ENDPT
}

const inDebugger = !!Number(process.env.INDEBUGGER)

const marshallOptions = {
  removeUndefinedValues: true
}
const dbClient = new DynamoDB(awsConfig)
// A DynamoDB Document Client instance without DAX integration
const documentClient = DynamoDBDocument.from(dbClient,
  { marshallOptions })

// istanbul ignore next
function tryMakeDaxClient () {
  if (inDebugger) {
    return // No DAX support in debugging / local
  }
  if (!process.env.DAX_ENDPOINT) {
    return // No endpoint configured
  }

  const AwsDaxClient = require('amazon-dax-client')
  const daxClient = new AwsDaxClient({
    endpoints: [process.env.DAX_ENDPOINT]
  })
  const daxV3 = new Proxy(daxClient, {
    get: (target, prop) => {
      if (typeof target[prop] === 'function') {
        return (...args) => target[prop](...args).promise()
      }
      return dbClient[prop]
    }
  })
  return DynamoDBDocument.from(daxV3, { marshallOptions })
}

const daxClient = tryMakeDaxClient()

const dbInstance = setup({
  dbClient,
  daxClient,
  documentClient
})
dbInstance.setupDB = setup

module.exports = dbInstance
