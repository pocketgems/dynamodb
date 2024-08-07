const AWSError = require('./aws-error')
const {
  InvalidCachedModelError, InvalidFilterError, InvalidOptionsError
} = require('./errors')
const Filter = require('./filter')
const { ITEM_SOURCE, loadOptionDefaults } = require('./utils')

function mergeCondition (base, additional) {
  const [condition, attrNames, attrValues] = base
  condition.push(...(additional[0] ?? []))
  Object.assign(attrNames, additional[1] ?? {})
  Object.assign(attrValues, additional[2] ?? {})
}

/**
 * Database iterator. Supports query and scan operations.
 * @private
 */
class __DBIterator {
  /**
   * Create an iterator instance.
   * @param {Object} params
   * @param {Class} params.ModelCls The model class.
   * @param {__WriteBatcher} params.writeBatcher An instance of __WriteBatcher
   * @param {Object} params.options Iterator options
   */
  constructor ({
    ModelCls,
    writeBatcher,
    options
  }) {
    this.__writeBatcher = writeBatcher
    this.__ModelCls = ModelCls
    this.__fetchParams = {}
    Object.assign(this, options)
    __DBIterator.__assertValidInputs(ModelCls, options)
  }

  static __assertValidInputs (ModelCls, options) {
    if (options.index) {
      if (options.inconsistentRead === false) {
        throw new InvalidOptionsError('index and inconsistent read. Reading from index needs to be inconsistent')
      }
      if (!ModelCls.INDEXES[options.index]) {
        throw new InvalidOptionsError(`index ${options.index}`)
      }
    }
  }

  static get METHOD () {
    return this.name.toLowerCase()
  }

  static __getKeyNames (Cls, index) {
    const extractKey = (name) => {
      if (index) {
        return new Set(Cls.INDEXES[index][name])
      }
      return new Set(Object.keys(Cls[name]))
    }
    const partitionKeys = extractKey('KEY')
    const sortKeys = extractKey('SORT_KEY')
    const allKeys = new Set([...partitionKeys, ...sortKeys])
    return {
      partitionKeys,
      sortKeys,
      allKeys
    }
  }

  __getKeyConditionExpression () {
    return [[], {}, {}] // conditions, attrNames, attrValues
  }

  __getFilterExpression () {
    return [[], {}, {}] // conditions, attrNames, attrValues
  }

  __addConditionExpression (params, methodName) {
    const [conditions, attrNames, attrValues] = this[methodName]()
    if (conditions.length > 0) {
      const type = methodName.replace('__get', '')
      params[type] = conditions.map(c => {
        return '(' + c + ')'
      }).join(' AND ')
      params.ExpressionAttributeNames = params.ExpressionAttributeNames ?? {}
      Object.assign(params.ExpressionAttributeNames, attrNames)
      params.ExpressionAttributeValues = params.ExpressionAttributeValues ??
          {}
      Object.assign(params.ExpressionAttributeValues, attrValues)
    }
  }

  __setupParams () {
    if (Object.keys(this.__fetchParams).length === 0) {
      // pagination and limit flags are setup else where since their values can
      // dynamically change as the iterator moves on.
      const params = {
        TableName: this.__ModelCls.fullTableName,
        ConsistentRead: !this.inconsistentRead
      }

      if (this.index) {
        params.IndexName = this.index
      }

      this.__addConditionExpression(params, '__getKeyConditionExpression')
      this.__addConditionExpression(params, '__getFilterExpression')
      if (this.shardIndex !== undefined) {
        params.Segment = this.shardIndex
        params.TotalSegments = this.shardCount
      }
      if (this.descending !== undefined) {
        params.ScanIndexForward = !this.descending
      }
      Object.assign(this.__fetchParams, params)
    }
    // Keep __fetchParams unchanged by shallow copying.
    // Only top level keys are modified (ExclusiveStartKey),
    // deepcopy is not necessary
    return { ...this.__fetchParams }
  }

  /**
   * Get one batch of items, by going through at most n items. Return a
   * nextToken for pagination.
   *
   * @param {Integer} n The max number of items to check (not return). When
   *   filtering is done, items not passing the filter conditions will not be
   *   returned, but they count towards the max.
   * @param {Object} [nextToken=undefined] A token for fetching the next batch.
   *   It is returned from a previous call to __getBatch. When nextToken is
   *   undefined, the function will go from the start of the DB table.
   *
   * @return {Tuple(Array<Model>, String)} A tuple of (items, nextToken). When
   *   nextToken
   *   is undefined, the end of the DB table has been reached.
   */
  async __getBatch (n, nextToken = undefined) {
    const params = this.__setupParams()
    params.Limit = n
    if (!nextToken) {
      delete params.ExclusiveStartKey
    } else {
      params.ExclusiveStartKey = nextToken
    }
    const method = this.constructor.METHOD
    const client = this.bypassCache ? this.documentClient : this.daxClient
    const result = await client[method](params)
      .catch(
        // istanbul ignore next
        e => { throw new AWSError(method, e) })

    const models = result.Items?.map(item => {
      const m = new this.__ModelCls(ITEM_SOURCE[method.toUpperCase()], false,
        item, this.index)
      if (m.__hasExpired) {
        return undefined
      }
      if (this.cacheModels) {
        const model = this.__writeBatcher.getModel(
          this.__ModelCls.fullTableName,
          m.__encodedKey._id,
          m.__encodedKey._sk
        )
        if (model) {
          if (!model.__src.canBeCached || model.__toBeDeleted) {
            throw new InvalidCachedModelError(model)
          }
          return model
        }
      }
      this.__writeBatcher.track(m)
      return m
    }).filter(m => !!m) || []

    return [
      models,
      result.LastEvaluatedKey
    ]
  }

  /**
   * Fetch n items from DB, return the fetched items and a token to next page.
   *
   * @param {Integer} n The number of items to return.
   * @param {String} [nextToken=undefined] A string token for fetching the next
   *   batch. It is returned from a previous call to fetch. When nextToken is
   *   undefined, the function will go from the start of the DB table.
   *
   * @return {Tuple(Array<Model>, String)} A tuple of (items, nextToken). When
   *   nextToken is undefined, the end of the DB table has been reached.
   */
  async fetch (n, nextToken = undefined) {
    const ret = []
    nextToken = nextToken ? JSON.parse(nextToken) : undefined
    while (ret.length < n) {
      const [ms, nt] = await this.__getBatch(
        n - ret.length,
        nextToken
      )
      ret.push(...ms)
      nextToken = nt // Update even if nt is undefined, to terminate pagination
      if (!nt) {
        // no more items
        break
      }
    }
    return [ret, nextToken ? JSON.stringify(nextToken) : undefined]
  }

  /**
   * A generator API for retrieving items from DB.
   *
   * @param {Integer} n The number of items to return.
   */
  async * run (n) {
    let fetchedCount = 0
    let nextToken
    while (fetchedCount < n) {
      const [models, nt] = await this.__getBatch(
        Math.min(n - fetchedCount, 50),
        nextToken
      )
      for (const model of models) {
        yield model
      }
      if (!nt) {
        return
      }
      fetchedCount += models.length
      nextToken = nt
    }
  }
}

/**
 * Scan handle for constructing filter expressions
 */
class Scan extends __DBIterator {
  /**
   * Create an iterator instance.
   * @param {Object} params
   * @param {Class} params.ModelCls The model class.
   * @param {__WriteBatcher} params.writeBatcher An instance of __WriteBatcher
   * @param {Object} params.options Iterator options
   * @property {Boolean} [params.options.inconsistentRead=false] When true,
   *   a strongly consistent read is performed.
   *   Note: Consistent read(inconsistentRead=false) only available when query
   *   or scan is performed on Model or LocalSecondaryIndex. To retrieve more
   *   recent results when scanning GlobalSecondaryIndex, enabling
   *   `bypassCache` instead.
   * @property {Number} [params.options.shardIndex=undefined] Only available in
   *   scan. Distributed scan is supported by dynamodb, where multiple machines
   *   can scan non-overlapping sections of the database in parallel, boosting
   *   the throughput of a database scan. ShardIndex denotes the shard where
   *   the scan should be performed on.
   * @property {Number} [params.options.shardCount=undefined] Only available in
   *   scan. Distributed scan is supported by dynamodb, where multiple machines
   *   can scan non-overlapping sections of the database in parallel, boosting
   *   the throughput of a database scan. ShardCount controls the number of
   *   shards in a distributed scan.
   * @property {Boolean} [params.options.bypassCache=false] DAX stores `Scan`
   *   and `Query` request results in its query cache, and the cache is not
   *   invalidated by DynamoDB updates. Set this to true to skip the DAX cache
   *   for more up-to-date results.
   * @property {Boolean} [params.options.cacheModels=false] Whether to cache
   *   models already retrieved from the database. When off, getting a model
   *   with the same key the second time in the same transaction results in an
   *   error. When on, `get`ting the same key simply returns the cached model.
   *   Previous modifications done to the model are reflected in the returned
   *   model.
   */
  constructor ({
    ModelCls,
    writeBatcher,
    options = {}
  }) {
    Scan.__assertValidInputs(options)
    options = loadOptionDefaults(options, {
      bypassCache: false,
      inconsistentRead: options.index !== undefined,
      shardCount: undefined,
      shardIndex: undefined,
      index: undefined,
      cacheModels: false
    })
    super({ ModelCls, writeBatcher, options })
    Object.freeze(this)
  }

  static __assertValidInputs (options) {
    if ((options.shardCount === undefined) !==
        (options.shardIndex === undefined)) {
      throw new InvalidOptionsError('shardIndex & shardCount',
        'Sharded scan requires both shardCount and shardIndex')
    }
    if (options.shardIndex < 0 || options.shardIndex >= options.shardCount) {
      throw new InvalidOptionsError('shardIndex',
        'ShardIndex must be positive and smaller than shardCount.')
    }
  }
}

class Query extends __DBIterator {
  /**
   * Create an iterator instance.
   * @param {Object} params
   * @param {Class} params.ModelCls The model class.
   * @param {__WriteBatcher} params.writeBatcher An instance of __WriteBatcher
   * @param {Object} params.options Iterator options
   * @param {Boolean} [params.options.allowLazyFilter=false] Filtering on
   *   non-key fields in query, and any fields in scan is performed after data
   *   is retrieved from db server node, but before data is returned to client.
   *   By default, this library forbids these filters by throwing exception as
   *   they cause slow operations. By allowing lazy filter, the exception is
   *   suppressed.
   * @param {Boolean} [params.options.descending=false] Only available in
   *   query, controls ordering based on sort key.
   * @property {Boolean} [params.options.inconsistentRead=false] When true,
   *   a strongly consistent read is performed.
   *   Note: Consistent read(inconsistentRead=false) only available when query
   *   or scan is performed on Model or LocalSecondaryIndex. To retrieve more
   *   recent results when scanning GlobalSecondaryIndex, enabling
   *   `bypassCache` instead.
   * @property {Boolean} [params.options.bypassCache=false] DAX stores `Scan`
   *   and `Query` request results in its query cache, and the cache is not
   *   invalidated by DynamoDB updates. Set this to true to skip the DAX cache
   *   for more up-to-date results.
   * @property {Boolean} [params.options.cacheModels=false] Whether to cache
   *   models already retrieved from the database. When off, getting a model
   *   with the same key the second time in the same transaction results in an
   *   error. When on, `get`ting the same key simply returns the cached model.
   *   Previous modifications done to the model are reflected in the returned
   *   model.
   */
  constructor ({
    ModelCls,
    writeBatcher,
    options = {}
  }) {
    options = loadOptionDefaults(options, {
      bypassCache: false,
      allowLazyFilter: false,
      descending: false,
      inconsistentRead: options.index !== undefined,
      index: undefined,
      cacheModels: false
    })
    super({ ModelCls, writeBatcher, options })
    this.__data = {}

    let index = 0
    // Use the PK, SK for the index if present
    this.__KEY_NAMES = this.constructor.__getKeyNames(ModelCls, this.index)
    const { partitionKeys, sortKeys } = this.__KEY_NAMES
    // We want to know the PK, SK for the model for lazy filtering
    const modelKeys = this.constructor.__getKeyNames(ModelCls)
    for (const name of Object.keys(ModelCls.schema.objectSchemas)) {
      let keyType
      if (partitionKeys.has(name)) {
        keyType = 'PARTITION'
      } else if (sortKeys.has(name)) {
        keyType = 'SORT'
      }
      let filterName = name
      if (this.index && !keyType && modelKeys.allKeys.has(name)) {
        // We are trying to filter on a field from the model's key
        filterName = ModelCls.__encodeCompoundFieldName([name])
      }
      const handle = new Filter(this.constructor.METHOD, filterName, `${index}`,
        keyType)
      this.__data[name] = handle
      if (!keyType && !this.allowLazyFilter) {
        this[name] = () => {
          throw new InvalidFilterError(`May not filter on non-key fields. You
can allow lazy filter to enable filtering non-key fields.`)
        }
      } else if (!keyType && filterName !== name && !ModelCls.__compoundFields.has(name)) {
        this[name] = () => {
          throw new InvalidFilterError(`May not filter on ${name}
          if INDEX_INCLUDE_KEYS is false`)
        }
      } else {
        const self = this
        this[name] = function (operation) {
          if (arguments.length === 1) {
            handle.filter('==', operation)
          } else {
            handle.filter(...arguments)
          }
          return self
        }
      }
      index++
    }
    Object.freeze(this)
  }

  __getEncodedVal (key, vals) {
    let funcName
    const pascalName = key[0].toUpperCase() + key.substring(1)
    if (this.index) {
      funcName = `__get${pascalName}ForIndex`
      return this.__ModelCls[funcName](vals, this.index)
    } else {
      funcName = `__get${pascalName}`
      return this.__ModelCls[funcName](vals)
    }
  }

  __getAWSKeyName (key) {
    if (this.index) {
      return this.__ModelCls.__getKeyNamesForIndex(this.index)[`_${key}`]
    }
    return `_${key}`
  }

  __getKeyConditionExpression () {
    const { partitionKeys, sortKeys } = this.__KEY_NAMES
    const keys = { id: partitionKeys, sk: sortKeys }
    const ret = [[], {}, {}]
    for (const keyName of ['id', 'sk']) {
      const awsKeyName = this.__getAWSKeyName(keyName)
      const keyComponents = {}
      let op
      for (const key of keys[keyName]) {
        const filter = this.__data[key]
        if (filter.__value !== undefined) {
          keyComponents[key] = filter.__value
          op = filter.awsOperator
        }
      }
      if (Object.keys(keyComponents).length === 0) {
        continue
      }
      if (op === 'between') {
        const betweenKeyComponents = [{}, {}]
        for (const [key, value] of Object.entries(keyComponents)) {
          for (let index = 0; index < value.length; index++) {
            betweenKeyComponents[index][key] = value[index]
          }
        }
        const [leftKeyComponents, rightKeyComponents] = betweenKeyComponents
        const leftValue = this.__getEncodedVal(keyName, leftKeyComponents)
        const rightValue = this.__getEncodedVal(keyName, rightKeyComponents)

        const condition = `#_${keyName} BETWEEN :l_${keyName} AND :r_${keyName}`
        mergeCondition(ret, [
          [condition],
          { [`#_${keyName}`]: `${awsKeyName}` },
          {
            [`:l_${keyName}`]: leftValue,
            [`:r_${keyName}`]: rightValue
          }
        ])
      } else {
        const value = this.__getEncodedVal(keyName, keyComponents)
        let condition = `#_${keyName}${op}:_${keyName}`
        if (op === 'prefix') {
          condition = `begins_with(#_${keyName},:_${keyName})`
        }
        mergeCondition(ret, [
          [condition],
          { [`#_${keyName}`]: `${awsKeyName}` },
          { [`:_${keyName}`]: value }
        ])
      }
    }
    if (ret[0].length === 0) {
      throw new InvalidFilterError('Query must contain partition key filters')
    }
    return ret
  }

  __getFilterExpression () {
    this.__checkKeyFilters()
    const ret = [[], {}, {}] // conditions, attrNames, attrValues
    const { partitionKeys, sortKeys } = this.__KEY_NAMES
    for (const [name, handle] of Object.entries(this.__data)) {
      if (partitionKeys.has(name)) {
        continue
      }
      if (sortKeys.has(name)) {
        continue
      }
      mergeCondition(ret, [
        handle.conditions,
        handle.attrNames,
        handle.attrValues
      ])
    }
    return ret
  }

  __checkKeyFilters () {
    const { partitionKeys, sortKeys } = this.__KEY_NAMES
    for (const keys of [partitionKeys, sortKeys]) {
      const operations = []
      for (const key of keys) {
        operations.push(this.__data[key].__operation)
      }
      const areOperationsSame = operations.reduce(
        (prev, curr) => prev && curr === operations[0],
        true
      )
      if (!areOperationsSame) {
        throw new InvalidFilterError(
          `Filter operations on keys ${Array.from(keys)} must be the same`)
      }
    }
  }

  __setupParams () {
    for (const handle of Object.values(this.__data)) {
      handle.lock()
    }
    return super.__setupParams()
  }
}

module.exports = {
  Query,
  Scan
}
