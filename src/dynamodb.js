const assert = require('assert')

const S = require('@pocketgems/schema')

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
const { UniqueKeyList } = require('./key')
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

const PROVISIONED_THROUGHPUT_UNCHANGED = 'The provisioned throughput for the table will not change.'

function makeCreateResourceFunc (dynamoDB, autoscaling) {
  return async function () {
    assert(dynamoDB,
      'Must provide dbClient when using createResources')
    if (Object.hasOwnProperty.call(this, '__createdResource')) {
      return // already created resource
    }
    this.__createdResource = true

    this.__doOneTimeModelPrep()
    const definitions = this.resourceDefinitions
    const tableParams = Object.values(definitions)
      .filter(val => val.Type === 'AWS::DynamoDB::Table')[0]
      .Properties
    const indexesProperties = tableParams.GlobalSecondaryIndexes ?? []
    if (!autoscaling) {
      tableParams.BillingMode = 'PAY_PER_REQUEST'
      delete tableParams.ProvisionedThroughput
      indexesProperties.forEach(each => delete each.ProvisionedThroughput)
    } else {
      tableParams.BillingMode = 'PROVISIONED'
      const config = {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1
      }
      tableParams.ProvisionedThroughput = config
      indexesProperties.forEach(each => { each.ProvisionedThroughput = config })
    }
    const ttlSpec = tableParams.TimeToLiveSpecification
    delete tableParams.TimeToLiveSpecification

    await dynamoDB.createTable(tableParams).catch(async err => {
      /* istanbul ignore if */
      if (err.name !== 'ResourceInUseException') {
        throw new AWSError('createTable', err)
      }

      // Update billing mode if needed for existing tables
      const tableDescription = await dynamoDB.describeTable({
        TableName: tableParams.TableName
      }).catch(
        // istanbul ignore next
        e => {
          throw new AWSError('describeTable', e)
        })

      let currentMode = tableDescription.Table.BillingModeSummary
        ?.BillingMode

      // per aws documentation, it is possible that BillingModeSummary
      // is omitted if the table was never set to PAY_PER_REQUEST
      // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BillingModeSummary.html
      /* istanbul ignore next */
      if (!currentMode && tableDescription.Table.ProvisionedThroughput) {
        currentMode = 'PROVISIONED'
      }
      if (currentMode !== tableParams.BillingMode) {
        const updateParams = { ...tableParams }
        delete updateParams.KeySchema
        delete updateParams.GlobalSecondaryIndexes

        await dynamoDB.updateTable(updateParams).catch(
          // istanbul ignore next
          e => {
            if (e.message?.indexOf(PROVISIONED_THROUGHPUT_UNCHANGED) !== 0) {
              throw new AWSError('Update Table', e)
            }
          })
      }
      const prevIndexes = new Set((tableDescription.Table.GlobalSecondaryIndexes ?? []).map(index => index.IndexName))
      const newIndexes = new Set((tableParams.GlobalSecondaryIndexes ?? []).map(index => index.IndexName))
      const updateParams = { ...tableParams }
      delete updateParams.BillingMode
      delete updateParams.KeySchema
      delete updateParams.ProvisionedThroughput
      delete updateParams.GlobalSecondaryIndexes
      const gsiUpdates = []

      for (const index of prevIndexes) {
        if (newIndexes.has(index) === false) {
          gsiUpdates.push({ Delete: { IndexName: index } })
        }
      }

      for (const index of tableParams.GlobalSecondaryIndexes ?? []) {
        if (prevIndexes.has(index.IndexName) === false) {
          gsiUpdates.push({ Create: index })
        }
      }

      if (gsiUpdates.length > 1) {
        throw new AWSError('Update Table',
          { message: 'Cannot modify more than one index at a time' })
      }

      if (gsiUpdates.length > 0) {
        updateParams.GlobalSecondaryIndexUpdates = gsiUpdates
        await dynamoDB.updateTable(updateParams)
      }
    })

    if (ttlSpec) {
      await dynamoDB.updateTimeToLive({
        TableName: tableParams.TableName,
        TimeToLiveSpecification: ttlSpec
      }).catch(
        /* istanbul ignore next */
        err => {
          if (err.message !== 'TimeToLive is already enabled') {
            throw new AWSError('updateTTL', err)
          }
        }
      )
    }

    if (autoscaling) {
      // create scalable target first
      for (const val of Object.values(definitions)) {
        const params = val.Properties
        if (val.Type === 'AWS::ApplicationAutoScaling::ScalableTarget') {
          delete params.RoleARN
          const targetsResult = await autoscaling.describeScalableTargets({
            ResourceIds: [params.ResourceId],
            ScalableDimension: params.ScalableDimension,
            ServiceNamespace: params.ServiceNamespace
          }).catch(
            // istanbul ignore next
            e => {
              throw new AWSError('describeScalableTargets', e)
            })

          // istanbul ignore else
          if (targetsResult.ScalableTargets.length === 0) {
            await autoscaling.registerScalableTarget(params)
              .catch(
                // istanbul ignore next
                e => {
                  throw new AWSError('registerScalableTarget', e)
                })
          }
        }
      }

      // create scaling policy second
      for (const val of Object.values(definitions)) {
        const params = val.Properties
        if (val.Type === 'AWS::ApplicationAutoScaling::ScalingPolicy') {
          params.ServiceNamespace = 'dynamodb'
          params.ResourceId = `table/${tableParams.TableName}`
          params.ScalableDimension = params.ScalingTargetId.Ref
            .includes('Write')
            ? 'dynamodb:table:WriteCapacityUnits'
            : 'dynamodb:table:ReadCapacityUnits'
          delete params.ScalingTargetId

          const policiesResult = await autoscaling.describeScalingPolicies({
            ServiceNamespace: params.ServiceNamespace,
            ResourceId: params.ResourceId,
            ScalableDimension: params.ScalableDimension
          }).catch(
            // istanbul ignore next
            e => {
              throw new AWSError('describeScalingPolicies', e)
            })
          // istanbul ignore else
          if (policiesResult.ScalingPolicies.length === 0) {
            await autoscaling.putScalingPolicy(params)
              .catch(
                // istanbul ignore next
                e => {
                  throw new AWSError('putScalingPolicy', e)
                })
          }
        }
      }
    }
  }
}

/* istanbul ignore next */
const DefaultConfig = {
  autoscalingClient: undefined,
  daxClient: undefined,
  dbClient: undefined,
  documentClient: undefined
}

// For backward compatibility
function renameSymbols (config) {
  const {
    dynamoDBClient: dbClient,
    dynamoDBDocumentClient: daxClient,
    documentClientWithoutDAX: documentClient,
    ...rest
  } = config
  return { dbClient, daxClient, documentClient, ...rest }
}

/**
 * @module dynamodb
 */

/**
 * Return a configured DB handle for caller to use.
 *
 * @param {Object} [config] Configurations for the library
 * @param {String} [config.documentClient=undefined] AWS DynamoDB document
 *   client, used for query and scan while bypassing DAX cache
 * @param {String} [config.daxClient=undefined] AWS DynamoDB DAX client used to
 *   interact with db items through DAX.
 * @param {Object} [config.dbClient=undefined] AWS DynamoDB Client used
 *   to manage table resources. Required when createResources is used.
 * @param {Object} [config.autoscalingClient=undefined] AWS Application
 *   AutoScaling client used to provision auto scaling rules on DB tables, if
 *   createResources is used.
 *
 * @returns {Object} Symbols that clients of this library can use.
 * @private
 */
function setup (config) {
  config = renameSymbols(config)
  config = loadOptionDefaults(config, DefaultConfig)

  Model.createResources = makeCreateResourceFunc(
    config.dbClient, config.autoscalingClient)

  // Make DynamoDB document clients available to these classes
  const daxClient = config.daxClient ?? config.documentClient
  const documentClient = config.documentClient
  const clsWithDBAccess = [
    __WriteBatcher,
    Model,
    Query,
    Scan,
    Transaction
  ]
  clsWithDBAccess.forEach(Cls => {
    Cls.dbClient = config.dbClient
    Cls.daxClient = daxClient
    Cls.prototype.daxClient = daxClient
    Cls.documentClient = documentClient
    Cls.prototype.documentClient = documentClient
  })

  const exportAsClass = {
    S,
    Model,
    UniqueKeyList,
    Transaction,
    AWSError,

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
