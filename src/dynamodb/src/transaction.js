const assert = require('assert')

const AsyncEmitter = require('./async-emitter')
const AWSError = require('./aws-error')
const { Data } = require('./data')
const {
  InvalidCachedModelError,
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
const { Scan } = require('./iterators')
const { Key } = require('./key')
const { Model, NonExistentItem } = require('./models')
const { sleep, ITEM_SOURCE, loadOptionDefaults } = require('./utils')

async function getWithArgs (args, callback) {
  if (!args || !(args instanceof Array) || args.length === 0) {
    throw new InvalidParameterError('args', 'must be a non-empty array')
  }
  const [first, ...args1] = args
  if (first && first.prototype instanceof Model) {
    if (args1.length === 1 || args1.length === 2) {
      let handle
      if (args1.length === 2 && args1[1].createIfMissing) {
        handle = first.data(args1[0])
      } else {
        handle = first.key(args1[0])
      }
      return getWithArgs([handle, ...args1.slice(1)], callback)
    } else {
      throw new InvalidParameterError('args',
        'Expecting args to have a tuple of (Model, values, optionalOpt).')
    }
  } else if (first && first instanceof Key) {
    if (args1.length > 1) {
      throw new InvalidParameterError('args',
        'Expecting args to have a tuple of (key, optionalOpt).')
    }
    return callback(first, args1.length === 1 ? args1[0] : undefined)
  } else if (first && first instanceof Array && first.length !== 0) {
    const nonKeys = first.filter(obj => !(obj instanceof Key))
    if (nonKeys.length !== 0) {
      throw new InvalidParameterError('args',
        'Expecting args to have a tuple of ([key], optionalOpt).')
    }
    if (args1.length > 1) {
      throw new InvalidParameterError('args',
        'Expecting args to have a tuple of ([key], optionalOpt).')
    }

    const params = args1.length === 1 ? args1[0] : undefined
    return callback(first, params)
  } else {
    console.log(JSON.stringify(args))
    throw new InvalidParameterError('args',
      'Expecting Model or Key or [Key] as the first argument')
  }
}

/**
 * Batches put and update (potentially could support delete) requests to
 * DynamoDB within a transaction and sent on commit.
 * @private
 * @memberof Internal
 *
 * @example
 * const batcher = new __WriteBatcher()
 * batcher.write(model)
 * batcher.write(otherModel)
 * await batcher.commit()
 */
class __WriteBatcher {
  constructor () {
    this.__allModels = []
    this.__toWrite = []
    this.__toCheck = {}
    this.resolved = false
  }

  /**
   * Gets params for the request according to method, batches the params.
   * Favors update over put for writing to DynamoDB, except for a corner case
   * where update disallows write operations without an UpdateExpression. This
   * happens when a new model is created with no fields besides keys populated
   * and written to DB.
   *
   * @param {Model} model the model to write
   * @access private
   */
  __write (model) {
    if (!this.__toCheck[model]) {
      if (this.__toCheck[model] === false) {
        throw new Error(`Attempting to write model ${model.toString()} twice`)
      } else {
        throw new Error('Attempting to write untracked model ' +
          model.toString())
      }
    }
    if (!model.__isMutated()) {
      throw new Error('Attempting to write an unchanged model ' +
        model.toString())
    }
    model.__finalize()
    this.__toCheck[model] = false

    let action
    let params

    if (model.__toBeDeleted) {
      action = 'Delete'
      params = model.__deleteParams()
    } else {
      action = 'Update'
      params = model.__updateParams()
      if (!Object.prototype.hasOwnProperty.call(
        params,
        'UpdateExpression'
      )) {
        // When a new item with no values other than the keys are written,
        // we have to use Put, else dynamodb would throw.
        action = 'Put'
        params = model.__putParams()
      }
    }
    this.__toWrite.push({ [action]: params })
  }

  /**
   * Start tracking models in a transaction. So when the batched write commits,
   * Optimistic locking on those readonly models is automatically performed.
   * @param {Model} model A model to track.
   */
  track (model) {
    const trackedModel = this.__toCheck[model]
    if (trackedModel !== undefined) {
      if (model.__src.isDelete) {
        throw new ModelDeletedTwiceError(model)
      } else if (!(model.__src.isGet || model.__src.isCreate) ||
                 !(trackedModel instanceof NonExistentItem)) {
        throw new ModelCreatedTwiceError(model)
      }
    }
    this.__allModels.push(model)
    this.__toCheck[model] = model
  }

  /**
   * Return all tracked models.
   */
  get trackedModels () {
    return Object.values(this.__allModels)
  }

  /**
   * Commits batched writes by sending DynamoDB requests.
   *
   * @returns {Boolean} whether any model is written to DB.
   */
  async commit (expectWrites) {
    assert(!this.resolved, 'Already wrote models.')
    this.resolved = true

    for (const model of this.__allModels) {
      if (this.__toCheck[model] && model.__isMutated()) {
        this.__write(model)
      }
    }

    if (this.__toWrite.length === 0) {
      return false
    }
    if (!expectWrites) {
      const x = this.__toWrite[0]
      let table, key
      if (x.Update) {
        table = x.Update.TableName
        key = x.Update.Key
      } else if (x.Put) {
        table = x.Put.TableName
        key = x.Put.Item
      } else {
        table = x.Delete.TableName
        key = x.Delete.Key
      }
      throw new WriteAttemptedInReadOnlyTxError(table, key._id, key._sk)
    }

    if (this.__allModels.length === 1 &&
        this.__toWrite.length === 1) {
      await this.__allModels[0].__write()
      return true
    }
    const toCheck = Object.values(this.__toCheck)
      .map(m => {
        if (m !== false) {
          return m.__conditionCheckParams()
        }
        return undefined
      })
      .filter(cond => !!cond)
      .map(cond => {
        return { ConditionCheck: cond }
      })
    const items = [...this.__toWrite, ...toCheck]
    const params = {
      TransactItems: items
    }
    await this.transactWrite(params)
    return true
  }

  async transactWrite (txWriteParams) {
    const request = this.documentClient.transactWrite(txWriteParams)
    request.on('extractError', (response) => {
      this.__extractError(request, response)
    })
    return request.promise().catch(e => {
      throw new AWSError('transactWrite', e)
    })
  }

  /**
   * Find a model with the same TableName and Key from a list of models
   * @param {String} tableName
   * @param {Object} key { _id: { S: '' }, _sk: { S: '' } }
   */
  __getModel (tableName, key) {
    const id = Object.values(key._id)[0]
    const sk = key._sk ? Object.values(key._sk)[0] : undefined
    return this.getModel(tableName, id, sk)
  }

  getModel (tableName, id, sk) {
    for (const model of this.__allModels) {
      if (model.__fullTableName === tableName &&
          model._id === id &&
          model._sk === sk) {
        return model
      }
    }
  }

  __extractError (request, response) {
    // istanbul ignore if
    if (response.httpResponse.body === undefined) {
      const { statusCode, statusMessage } = response.httpResponse
      console.log(`error code ${statusCode}, message ${statusMessage}`)
      return
    }

    const responseBody = response.httpResponse.body.toString()
    const reasons = JSON.parse(responseBody).CancellationReasons
    assert(reasons, 'error body missing reasons: ' + responseBody)
    if (response.error) {
      response.error.allErrors = []
    }
    for (let idx = 0; idx < reasons.length; idx++) {
      const reason = reasons[idx]
      if (reason.Code === 'ConditionalCheckFailed') {
        // Items in reasons maps 1to1 to items in request, here we do a reverse
        // lookup to find the original model that triggered the error.
        const transact = request.params.TransactItems[idx]
        const method = Object.keys(transact)[0]
        let model
        const tableName = transact[method].TableName
        switch (method) {
          case 'Update':
          case 'ConditionCheck':
          case 'Delete':
            model = this.__getModel(
              tableName,
              transact[method].Key
            )
            break
          case 'Put':
            model = this.__getModel(
              tableName,
              transact[method].Item
            )
            break
        }
        let CustomErrorCls
        if (model.__toBeDeleted) {
          CustomErrorCls = InvalidModelDeletionError
        } else if (model.__src.isCreate) {
          CustomErrorCls = ModelAlreadyExistsError
        } else if (model.__src.isUpdate) {
          CustomErrorCls = InvalidModelUpdateError
        }
        if (CustomErrorCls) {
          const err = new CustomErrorCls(tableName, model._id, model._sk)
          if (response.error) {
            response.error.allErrors.push(err)
          } else {
            // response.error appears to always be set in the wild; but we have
            // this case just in case we're wrong or something changes
            response.error = err
            response.error.allErrors = [err]
          }
        }
      }
    }
    // if there were no custom errors, then use the original error
    /* istanbul ignore if */
    if (response.error && !response.error.allErrors.length) {
      response.error.allErrors.push(response.error)
    }
  }
}

/**
 * Transaction context.
 */
class Transaction {
  /**
   * Options for running a transaction.
   * @typedef {Object} TransactionOptions
   * @property {Boolean} [readOnly=false] whether writes are allowed
   * @property {Number} [retries=3] The number of times to retry after the
   *   initial attempt fails.
   * @property {Number} [initialBackoff=500] In milliseconds, delay
   *   after the first attempt fails and before first retry happens.
   * @property {Number} [maxBackoff=10000] In milliseconds, max delay
   *   between retries. Must be larger than 200.
   * @property {Number} [cacheModels=false] Whether to cache models already
   *   retrieved from the database. When off, getting a model with the same key
   *   the second time in the same transaction results in an error. When on,
   *   `get`ting the same key simply returns the cached model. Previous
   *   modifications done to the model are reflected in the returned model. If
   *   the model key was used in some API other than "get", an error will
   *   result.
   */

  /**
   * Returns the default [options]{@link TransactionOptions} for a transaction.
   */
  get defaultOptions () {
    return {
      readOnly: false,
      retries: 3,
      initialBackoff: 500,
      maxBackoff: 10000,
      cacheModels: false
    }
  }

  /**
   * @param {TransactionOptions} [options] Options for the transaction
   */
  constructor (options) {
    const defaults = this.defaultOptions
    this.options = loadOptionDefaults(options, defaults)

    if (this.options.retries < 0) {
      throw new InvalidOptionsError('retries',
        'Retry count must be non-negative')
    }
    if (this.options.initialBackoff < 1) {
      throw new InvalidOptionsError('initialBackoff',
        'Initial back off must be larger than 1ms.')
    }
    if (this.options.maxBackoff < 200) {
      // A transactWrite would take some where between 100~200ms.
      // Max of less than 200 is too aggressive.
      throw new InvalidOptionsError('maxBackoff',
        'Max back off must be larger than 200ms.')
    }
  }

  /**
   * All events transactions may emit.
   *
   * POST_COMMIT: When a transaction is committed. Do clean up,
   *              summery, post process here.
   * TX_FAILED: When a transaction failed permanently (either by failing all
   *            retries, or getting a non-retryable error). Handler has the
   *            signature of (error) => {}.
   */
  static EVENTS = {
    POST_COMMIT: 'postCommit',
    TX_FAILED: 'txFailed'
  }

  addEventHandler (event, handler, name = undefined) {
    if (!Object.values(this.constructor.EVENTS).includes(event)) {
      throw new Error(`Unsupported event ${event}`)
    }
    this.__eventEmitter.once(event, handler, name)
  }

  /**
   * Get an item using DynamoDB's getItem API.
   *
   * @param {Key} key A key for the item
   * @param {GetParams} params Params for how to get the item
   */
  async __getItem (key, params) {
    const getParams = key.Cls.__getParams(key.encodedKeys, params)
    const data = await this.documentClient.get(getParams).promise()
      .catch(
        // istanbul ignore next
        e => {
          throw new AWSError('get', e)
        })
    if (!params.createIfMissing && !data.Item) {
      this.__writeBatcher.track(new NonExistentItem(key))
      return undefined
    }
    const isNew = !data.Item
    const vals = data.Item || key.vals
    let model = new key.Cls(ITEM_SOURCE.GET, isNew, vals)
    if (model.__hasExpired) {
      // DynamoDB may not have deleted the model promptly, just treat it as if
      // it's not on server.
      if (params.createIfMissing) {
        model = new key.Cls(ITEM_SOURCE.GET, true, key.vals)
      } else {
        this.__writeBatcher.track(new NonExistentItem(key))
        return undefined
      }
    }
    this.__writeBatcher.track(model)
    return model
  }

  /**
   * Gets multiple items using DynamoDB's transactGetItems API.
   * @param {Array<Key>} keys A list of keys to get.
   * @param {GetParams} params Params used to get items, all items will be
   *   fetched using the same params.
   */
  async __transactGetItems (keys, params) {
    const txItems = []
    for (const key of keys) {
      const param = key.Cls.__getParams(key.encodedKeys, params)
      delete param.ConsistentRead // Omit for transactGetItems.
      txItems.push({
        Get: param
      })
    }
    const data = await this.documentClient.transactGet({
      TransactItems: txItems
    }).promise().catch(
      // istanbul ignore next
      e => { throw new AWSError('transactGet', e) }
    )
    const responses = data.Responses
    const models = []
    for (let idx = 0; idx < keys.length; idx++) {
      const data = responses[idx]
      if ((!params || !params.createIfMissing) && !data.Item) {
        models[idx] = undefined
        continue
      }
      const key = keys[idx]
      let model = new key.Cls(
        ITEM_SOURCE.GET,
        !data.Item,
        data.Item || key.vals)

      if (model.__hasExpired) {
        if (params.createIfMissing) {
          model = new key.Cls(
            ITEM_SOURCE.GET,
            true,
            key.vals)
        } else {
          model = undefined
        }
      }

      models[idx] = model
      if (model) {
        this.__writeBatcher.track(model)
      }
    }
    return models
  }

  /**
   * Gets multiple items using DynamoDB's batchGetItems API.
   * @param {Array<Key>} keys A list of keys to get.
   * @param {GetParams} params Params used to get items, all items will be
   *   fetched using the same params.
   */
  async __batchGetItems (keys, params) {
    let reqItems = {}
    const unorderedModels = []
    const modelClsLookup = {}
    for (const key of keys) {
      modelClsLookup[key.Cls.fullTableName] = key.Cls
      const param = key.Cls.__getParams(key.encodedKeys, params)
      const getsPerTable = reqItems[param.TableName] || { Keys: [] }
      getsPerTable.Keys.push(param.Key)
      getsPerTable.ConsistentRead = param.ConsistentRead
      reqItems[param.TableName] = getsPerTable
    }

    let reqCnt = 0
    while (Object.keys(reqItems).length !== 0) {
      if (reqCnt > 10) {
        throw new Error(`Failed to get all items ${
          keys.map(k => {
            return `${k.Cls.name} ${JSON.stringify(k.compositeID)}`
          })}`)
      }
      if (reqCnt !== 0) {
        // Backoff
        const millisBackOff = Math.min(100 * reqCnt, 1000)
        const offset = Math.floor(Math.random() * millisBackOff * 0.2) -
        millisBackOff * 0.1 // +-0.1 backoff as jitter to spread out conflicts
        await sleep(millisBackOff + offset)
      }
      reqCnt++

      const data = await this.documentClient.batchGet({
        RequestItems: reqItems
      }).promise().catch(
        // istanbul ignore next
        e => { throw new AWSError('batchGet', e) }
      )

      // Merge results
      const responses = data.Responses
      for (const [modelClsName, items] of Object.entries(responses)) {
        const Cls = modelClsLookup[modelClsName]
        for (const item of items) {
          const tempModel = new Cls(ITEM_SOURCE.GET, false, item)
          if (!tempModel.__hasExpired) {
            unorderedModels.push(tempModel)
          }
        }
      }

      // Chain into next batch
      reqItems = data.UnprocessedKeys
    }

    // Restore ordering, creat models that are not on server.
    const models = []
    for (let idx = 0; idx < keys.length; idx++) {
      const key = keys[idx]
      const addModel = () => {
        for (const model of unorderedModels) {
          if (model.__fullTableName === key.Cls.fullTableName &&
              model._id === key.encodedKeys._id &&
              model._sk === key.encodedKeys._sk) {
            models.push(model)
            return true
          }
        }
        return false
      }
      if (addModel()) {
        continue
      }
      // If we reach here, no model is found for the key.
      if (params.createIfMissing) {
        models.push(new key.Cls(ITEM_SOURCE.GET, true, key.vals))
      } else {
        models.push(undefined)
      }
    }

    // Now track models, so everything is in expected order.
    models.forEach(model => {
      if (model) {
        this.__writeBatcher.track(model)
      }
    })
    return models
  }

  /**
   * Fetches model(s) from database.
   * This method supports 3 different signatures.
   *   get(Cls, keyOrDataValues, params)
   *   get(Key|Data, params)
   *   get([Key|Data], params)
   *
   * When only one items is fetched, DynamoDB's getItem API is called. Must use
   * a Key when createIfMissing is not true, and Data otherwise.
   *
   * When a list of items is fetched:
   *   If inconsistentRead is false (the default), DynamoDB's transactGetItems
   *     API is called for a strongly consistent read. Transactional reads will
   *     be slower than batched reads.
   *   If inconsistentRead is true, DynamoDB's batchGetItems API is called.
   *     Batched fetches are more efficient than calling get with 1 key many
   *     times, since there is less HTTP request overhead. Batched fetches is
   *     faster than transactional fetches, but provides a weaker consistency.
   *
   * @param {Class} Cls a Model class.
   * @param {String|CompositeID} key Key or keyValues
   * @param {GetParams} [params]
   * @returns Model(s) associated with provided key
   */
  async get (...args) {
    return getWithArgs(args, async (arg, params) => {
      // make sure we have a Key or Data depending on createIfMissing
      params = params || {}
      const argIsArray = arg instanceof Array
      const arr = argIsArray ? arg : [arg]
      for (let i = 0; i < arr.length; i++) {
        if (params.createIfMissing) {
          if (!(arr[i] instanceof Data)) {
            throw new InvalidParameterError('args',
              'must pass a Data to tx.get() when createIfMissing is true')
          }
        } else if (arr[i] instanceof Data) {
          throw new InvalidParameterError('args',
            'must pass a Key to tx.get() when createIfMissing is not true')
        }
      }
      const cachedModels = []
      let keysOrDataToGet = []
      if (this.options.cacheModels) {
        for (const keyOrData of arr) {
          const cachedModel = this.__writeBatcher.getModel(
            keyOrData.Cls.fullTableName,
            keyOrData.encodedKeys._id,
            keyOrData.encodedKeys._sk
          )
          if (cachedModel) {
            if (!cachedModel.__src.isGet || cachedModel.__toBeDeleted) {
              throw new InvalidCachedModelError(cachedModel)
            }
            cachedModels.push(cachedModel)
          } else {
            keysOrDataToGet.push(keyOrData)
          }
        }
      } else {
        keysOrDataToGet = arr
      }
      // fetch the data in bulk if more than 1 item was requested
      const fetchedModels = []
      if (keysOrDataToGet.length > 0) {
        if (argIsArray) {
          if (!params.inconsistentRead) {
            fetchedModels.push(
              ...await this.__transactGetItems(keysOrDataToGet, params))
          } else {
            fetchedModels.push(
              ...await this.__batchGetItems(keysOrDataToGet, params))
          }
        } else {
          // just fetch the one item that was requested
          fetchedModels.push(await this.__getItem(keysOrDataToGet[0], params))
        }
      }

      let ret = []
      if (this.options.cacheModels) {
        const findModel = (tableName, id, sk) => {
          for (let index = 0; index < keysOrDataToGet.length; index++) {
            const toGetKeyOrData = keysOrDataToGet[index]
            if (tableName === toGetKeyOrData.Cls.fullTableName &&
              id === toGetKeyOrData.encodedKeys._id &&
              sk === toGetKeyOrData.encodedKeys._sk) {
              return fetchedModels[index]
            }
          }

          for (const model of cachedModels) {
            // istanbul ignore else
            if (tableName === model.constructor.fullTableName &&
              id === model._id &&
              sk === model._sk) {
              return model
            }
          }
        }
        for (const keyOrData of arr) {
          ret.push(findModel(
            keyOrData.Cls.fullTableName,
            keyOrData.encodedKeys._id,
            keyOrData.encodedKeys._sk
          ))
        }
      } else {
        // UnorderedModels is really ordered when cacheModels is disabled
        // don't sort to save time
        ret = fetchedModels
      }

      return argIsArray ? ret : ret[0]
    })
  }

  /**
   * Updates an item without reading from DB. If the item doesn't exist in DB,
   * ConditionCheckFailure will be thrown.
   *
   * @param {Class} Cls The model's class.
   * @param {CompositeID|Object} original A superset of CompositeID,
   *   field's values. Used as conditions for the update
   * @param {Object} updated Updated fields for the item, without CompositeID
   *   fields.
   */
  update (Cls, original, updated) {
    if (Object.values(original).filter(d => d === undefined).length !== 0) {
      // We don't check for attribute_not_exists anyway.
      throw new InvalidParameterError(
        'original',
        'original values must not be undefined')
    }
    if (!updated || Object.keys(updated).length === 0) {
      throw new InvalidParameterError(
        'updated',
        'must have values to be updated')
    }

    const data = Cls.__splitKeysAndData(original)[2] // this also checks keys
    const model = new Cls(ITEM_SOURCE.UPDATE, false, original)
    Object.keys(data).forEach(k => {
      model.getField(k).get() // Read to show in ConditionExpression
    })

    Object.keys(updated).forEach(key => {
      if (Cls._attrs[key].keyType !== undefined) {
        throw new InvalidParameterError(
          'updated', 'must not contain key fields')
      }
      model[key] = updated[key]
    })

    this.__writeBatcher.track(model)

    // Don't return model, since it should be closed to further modifications.
    // return model
  }

  /**
   * Creates or puts an item without reading from DB.
   * It differs from {@link update} in that:
   *   a) If item doesn't exists, a new item is created in DB
   *   b) If item does exists, fields present locally will overwrite values in
   *      DB, fields absent locally will be removed from DB.
   *
   * @param {Class} Cls The model's class.
   * @param {CompositeID|Object} original A superset of CompositeID,
   *   field's values. Non-key values are used for conditional locking
   * @param {Object} updated Final values for the model.
   *   Values for every field in the model must be provided. Fields with
   *   `undefined` value will be removed from DB.
   */
  createOrPut (Cls, original, updated) {
    const newData = { ...updated }
    for (const key of Object.keys(original)) {
      if (Object.hasOwnProperty.call(newData, key)) {
        // cannot change a key component's value
        if (Cls.__KEY_COMPONENT_NAMES.has(key)) {
          if (newData[key] !== original[key]) {
            throw new InvalidParameterError(updated,
              'key components values in updated must match (or be omitted)')
          }
        }
      } else {
        // old values which aren't explicitly changed are kept the same
        newData[key] = original[key]
      }
    }
    // We create the item we intend to write (with newData), and then update
    // its __initialValue for any preconditions requested (with `original`).
    // Creating the model with newData validates that newData specified are
    // complete, valid item all on its own.
    const model = new Cls(ITEM_SOURCE.CREATE_OR_PUT, true, newData)
    Object.keys(original).forEach(key => {
      const field = model.getField(key)
      // we set the initial value and then mark it as read so that the write
      // batcher later generates a database update which is conditioned on the
      // the item's current value in the database for this field being
      // original[key] (if the item existed)
      field.__initialValue = original[key]
      field.get()
    })
    this.__writeBatcher.track(model)

    // Don't return model, since it should be closed to further modifications.
    // return model
  }

  /**
   * Creates a model without accessing DB. Write will make sure the item does
   * not exist.
   *
   * @param {Model} Cls A Model class.
   * @param {CompositeID|Object} data A superset of CompositeID of the model,
   *   plus any data for Fields on the Model.
   */
  create (Cls, data) {
    const model = new Cls(ITEM_SOURCE.CREATE, true, { ...data })
    this.__writeBatcher.track(model)
    return model
  }

  /**
   * Deletes model(s) from database.
   *
   * If a model is read from database, but it did not exist when deleting the
   * item, an exception is raised.
   *
   * @param {List<Key|Model>} args Keys and Models
   */
  delete (...args) {
    for (const a of args) {
      if (a instanceof Model) {
        a.__markForDeletion()
      } else if (a instanceof Key) {
        const model = new a.Cls(ITEM_SOURCE.DELETE, true,
          a.keyComponents)
        this.__writeBatcher.track(model)
      } else {
        throw new InvalidParameterError('args', 'Must be models and keys')
      }
    }
  }

  /**
   * Create a handle for applications to scan DB items.
   * @param {Model} Cls A model class.
   * @param {Object} options
   * @param {Object} options.inconsistentRead Whether to do a strong consistent
   *   read. Default to false.
   * @return Scan handle. See {@link Scan} for details.
   */
  scan (Cls, options) {
    return new Scan({
      Cls,
      writeBatcher: this.__writeBatcher,
      options
    })
  }

  __reset () {
    this.__writeBatcher = new __WriteBatcher()
    this.__eventEmitter = new AsyncEmitter()
  }

  static __isRetryable (err) {
    const retryableErrors = {
      ConditionalCheckFailedException: true,
      TransactionCanceledException: true
    }

    if (err.retryable) {
      return true
    }

    if (retryableErrors[err.code]) {
      return true
    }
    return false
  }

  /** Marks a transaction as read-only. */
  makeReadOnly () {
    this.options.readOnly = true
  }

  /** Enables model cache */
  enableModelCache () {
    this.options.cacheModels = true
  }

  /**
   * Runs a closure in transaction.
   * @param {Function} func the closure to run
   * @access private
   */
  async __run (func) {
    if (!(func instanceof Function || typeof func === 'function')) {
      throw new InvalidParameterError('func', 'must be a function / closure')
    }

    let millisBackOff = this.options.initialBackoff
    const maxBackoff = this.options.maxBackoff
    for (let tryCnt = 0; tryCnt <= this.options.retries; tryCnt++) {
      try {
        this.__reset()
        const ret = await func(this)
        await this.__writeBatcher.commit(!this.options.readOnly)
        await this.__eventEmitter.emit(this.constructor.EVENTS.POST_COMMIT)
        return ret
      } catch (err) {
        // make sure EVERY error is retryable; allErrors is present if err
        // was thrown in __WriteBatcher.commit()'s onError handler
        const allErrors = err.allErrors || [err]
        const errorMessages = []
        for (let i = 0; i < allErrors.length; i++) {
          const anErr = allErrors[i]
          if (!this.constructor.__isRetryable(anErr)) {
            errorMessages.push(`  ${i + 1}) ${anErr.message}`)
          }
        }
        if (errorMessages.length) {
          if (allErrors.length === 1) {
            // if there was only one error, just rethrow it
            const e = allErrors[0]
            await this.__eventEmitter.emit(this.constructor.EVENTS.TX_FAILED,
              e)
            throw e
          } else {
            // if there were multiple errors, combine it into one error which
            // summarizes all of the failures
            const e = new TransactionFailedError(
              ['Multiple Non-retryable Errors: ', ...errorMessages].join('\n'),
              err)
            await this.__eventEmitter.emit(this.constructor.EVENTS.TX_FAILED,
              e)
            throw e
          }
        } else {
          console.log(`Transaction commit attempt ${tryCnt} failed with ` +
            `error ${err}.`)
        }
      }
      if (tryCnt >= this.options.retries) {
        // note: this exact message is checked and during load testing this
        // error will not be sent to Sentry; if this message changes, please
        // update make-app.js too
        const err = new TransactionFailedError('Too much contention.')
        await this.__eventEmitter.emit(this.constructor.EVENTS.TX_FAILED, err)
        throw err
      }
      const offset = Math.floor(Math.random() * millisBackOff * 0.2) -
        millisBackOff * 0.1 // +-0.1 backoff as jitter to spread out conflicts
      await sleep(millisBackOff + offset)
      millisBackOff = Math.min(maxBackoff, millisBackOff * 2)
    }
  }

  /**
   * Runs a function in transaction, using specified parameters.
   *
   * If a non-retryable error is thrown while running the transaction, it will
   * be re-raised.
   *
   * @param {TransactionOptions} [options]
   * @param {Function} func the closure to run.
   *
   * @example
   * // Can be called in 2 ways:
   * Transaction.run(async (tx) => {
   *   // Do something
   * })
   *
   * // Or
   * Transaction.run({ retryCount: 2 }, async (tx) => {
   *   // Do something
   * })
   */
  static async run (...args) {
    const opts = (args.length === 1) ? {} : args[0]
    const func = args[args.length - 1]
    if (args.length <= 0 || args.length > 2) {
      throw new InvalidParameterError('args', 'should be ([options,] func)')
    }
    return new Transaction(opts).__run(func)
  }

  /**
   * Return before and after snapshots of all relevant models.
   */
  getModelDiffs (filter = () => true) {
    const allBefore = []
    const allAfter = []
    for (const model of this.__writeBatcher.trackedModels) {
      if (!model.getSnapshot || !filter(model)) {
        continue
      }
      const before = model.getSnapshot({ initial: true, dbKeys: true })
      const after = model.getSnapshot({ initial: false, dbKeys: true })
      const key = model.constructor.name
      allBefore.push({ [key]: before })
      allAfter.push({ [key]: after })
    }
    return {
      before: allBefore,
      after: allAfter
    }
  }
}

module.exports = {
  __WriteBatcher,
  Transaction,
  getWithArgs
}
