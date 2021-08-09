const assert = require('assert')

const S = require('../../schema/src/schema')

const {
  InvalidCachedModelError,
  InvalidFieldError,
  InvalidModelDeletionError,
  InvalidModelUpdateError,
  InvalidOptionsError,
  InvalidParameterError,
  ModelAlreadyExistsError,
  ModelCreatedTwiceError,
  ModelDeletedTwiceError,
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
const { Scan } = require('./iterators')
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

function makeCreateResourceFunc (dynamoDB) {
  return async function () {
    this.__doOneTimeModelPrep()
    const params = this.__getResourceDefinition()
    const ttlSpec = params.TimeToLiveSpecification
    delete params.TimeToLiveSpecification

    await dynamoDB.createTable(params).promise().catch(err => {
      /* istanbul ignore if */
      if (err.code !== 'ResourceInUseException') {
        throw err
      }
    })
    if (ttlSpec) {
      await dynamoDB.updateTimeToLive({
        TableName: params.TableName,
        TimeToLiveSpecification: ttlSpec
      }).promise().catch(
        /* istanbul ignore next */
        err => {
          if (err.message !== 'TimeToLive is already enabled') {
            throw err
          }
        }
      )
    }
  }
}

/* istanbul ignore next */
const DefaultConfig = {
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
      config.dynamoDBClient)
  }

  // Make DynamoDB document client available to these classes
  const documentClient = config.dynamoDBDocumentClient
  const clsWithDBAccess = [
    Model,
    Transaction,
    __WriteBatcher,
    Scan
  ]
  clsWithDBAccess.forEach(Cls => {
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
    ModelCreatedTwiceError,
    ModelDeletedTwiceError,
    ModelAlreadyExistsError,
    TransactionFailedError,
    WriteAttemptedInReadOnlyTxError
  }

  const toExport = Object.assign({}, exportAsClass)
  if (Number(process.env.INDEBUGGER)) {
    toExport.__private = {
      __Field,
      __WriteBatcher,
      getWithArgs,
      fields: [
        ArrayField,
        BooleanField,
        NumberField,
        ObjectField,
        StringField
      ],
      ITEM_SOURCE
    }
  }
  return toExport
}

module.exports = setup
