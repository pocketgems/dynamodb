const assert = require('assert')

const S = require('../../schema/src/schema')

const AWSError = require('./aws-error')
const {
  InvalidCachedModelError,
  InvalidFieldError,
  InvalidModelDeletionError,
  InvalidModelUpdateError,
  InvalidOptionsError,
  InvalidParameterError,
  ModelAlreadyExistsError,
  ModelDeletedTwiceError,
  ModelTrackedTwiceError,
  TransactionFailedError,
  WriteAttemptedInReadOnlyTxError
} = require('./errors')
const {
  __Field,
  ArrayField,
  BooleanField,
  NumberField,
  ObjectField,
  StringField
} = require('./fields')
const Filter = require('./filter')
const { Query, Scan } = require('./iterators')
const { Model } = require('./models')
const {
  __WriteBatcher,
  getWithArgs,
  Transaction
} = require('./transaction')
const {
  ITEM_SOURCE,
  loadOptionDefaults
} = require('./utils')

function makeCreateResourceFunc (dynamoDB, autoscaling) {
  return async function () {
    this.__doOneTimeModelPrep()
    const definitions = this.__getResourceDefinitions()
    const tableParams = Object.values(definitions)
      .filter(val => val.Type === 'AWS::DynamoDB::Table')[0]
      .Properties
    if (!autoscaling) {
      tableParams.BillingMode = 'PAY_PER_REQUEST'
      delete tableParams.ProvisionedThroughput
    } else {
      tableParams.BillingMode = 'PROVISIONED'
      tableParams.ProvisionedThroughput = {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1
      }
    }
    const ttlSpec = tableParams.TimeToLiveSpecification
    delete tableParams.TimeToLiveSpecification

    await dynamoDB.createTable(tableParams).promise().catch(async err => {
      /* istanbul ignore if */
      if (err.code !== 'ResourceInUseException') {
        throw new AWSError('createTable', err)
      }

      // Update billing mode if needed for existing tables
      const tableDescription = await dynamoDB.describeTable({
        TableName: tableParams.TableName
      }).promise()
      const currentMode = tableDescription.Table.BillingModeSummary
        ?.BillingMode
      if (currentMode !== tableParams.BillingMode) {
        const updateParams = { ...tableParams }
        delete updateParams.KeySchema
        await dynamoDB.updateTable(updateParams).promise().catch(e => {
          // istanbul ignore if
          if (e.message !==
            'The requested throughput value equals the current value') {
            throw e
          }
        })
      }
    })

    if (ttlSpec) {
      await dynamoDB.updateTimeToLive({
        TableName: tableParams.TableName,
        TimeToLiveSpecification: ttlSpec
      }).promise().catch(
        /* istanbul ignore next */
        err => {
          if (err.message !== 'TimeToLive is already enabled') {
            throw new AWSError('updateTTL', err)
          }
        }
      )
    }

    // istanbul ignore if
    if (autoscaling) {
      for (const val of Object.values(definitions)) {
        const params = val.Properties
        if (val.type === 'AWS::ApplicationAutoScaling::ScalableTarget') {
          delete params.RoleARN
          await autoscaling.registerScalableTarget(params).promise()
        } else if (val.type === 'AWS::ApplicationAutoScaling::ScalingPolicy') {
          await autoscaling.putScalingPolicy(params).promise()
        }
      }
    }
  }
}

/* istanbul ignore next */
const DefaultConfig = {
  autoscalingClient: undefined,
  dynamoDBClient: undefined,
  dynamoDBDocumentClient: undefined,
  enableDynamicResourceCreation: false
}

/**
 * @module dynamodb
 */

/**
 * Setup the DynamoDB library before returning symbols clients can use.
 *
 * @param {Object} [config] Configurations for the library
 * @param {Object} [config.dynamoDBClient=undefined] AWS DynamoDB Client used
 *   to manage table resources. Required when enableDynamicResourceCreation is
 *   true.
 * @param {String} [config.dynamoDBDocumentClient] AWS DynamoDB document client
 *   used to interact with db items.
 * @param {Object} [config.autoscalingClient=undefined] AWS Application
 *   AutoScaling client used to provision auto scaling rules on DB tables.
 * @param {Boolean} [config.enableDynamicResourceCreation=false] Wether to
 *   enable dynamic table resource creations.
 * @returns {Object} Symbols that clients of this library can use.
 * @private
 */
function setup (config) {
  config = loadOptionDefaults(config, DefaultConfig)

  if (config.enableDynamicResourceCreation) {
    assert(config.dynamoDBClient,
      'Must provide dynamoDBClient when enableDynamicResourceCreation is on')
    Model.createResource = makeCreateResourceFunc(
      config.dynamoDBClient, config.autoscalingClient)
  }

  // Make DynamoDB document client available to these classes
  const documentClient = config.dynamoDBDocumentClient
  const clsWithDBAccess = [
    __WriteBatcher,
    Model,
    Query,
    Scan,
    Transaction
  ]
  clsWithDBAccess.forEach(Cls => {
    Cls.dbClient = config.dynamoDBClient
    Cls.documentClient = documentClient
    Cls.prototype.documentClient = documentClient
  })

  const exportAsClass = {
    S,
    Model,
    Transaction,

    // Errors
    InvalidFieldError,
    InvalidModelDeletionError,
    InvalidModelUpdateError,
    InvalidCachedModelError,
    InvalidOptionsError,
    InvalidParameterError,
    ModelDeletedTwiceError,
    ModelTrackedTwiceError,
    ModelAlreadyExistsError,
    TransactionFailedError,
    WriteAttemptedInReadOnlyTxError
  }

  const toExport = Object.assign({}, exportAsClass)
  if (Number(process.env.INDEBUGGER)) {
    toExport.__private = {
      __Field,
      __WriteBatcher,
      Filter,
      fields: [
        ArrayField,
        BooleanField,
        NumberField,
        ObjectField,
        StringField
      ],
      getWithArgs,
      ITEM_SOURCE,
      Query,
      Scan
    }
  }
  return toExport
}

module.exports = setup
