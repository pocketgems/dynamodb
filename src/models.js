const assert = require('assert')

const S = require('@pocketgems/schema')
const stableStringify = require('fast-json-stable-stringify')
const deepcopy = require('rfdc')()

const AWSError = require('./aws-error')
const { Data } = require('./data')
const {
  GenericModelError,
  InvalidFieldError,
  InvalidModelDeletionError,
  InvalidModelUpdateError,
  InvalidParameterError,
  InvalidIndexError,
  ModelAlreadyExistsError,
  ModelDeletedTwiceError
} = require('./errors')
const { __Field, SCHEMA_TYPE_TO_FIELD_CLASS_MAP, __CompoundField } = require('./fields')
const { Key } = require('./key')
const {
  validateValue,
  ITEM_SOURCES,
  makeItemString,
  SCHEMA_TYPE_TO_JS_TYPE_MAP,
  sleep
} = require('./utils')

/**
 * The base class for modeling data.
 */
class Model {
  /**
   * Create a representation of a database Item. Should only be used by the
   * library.
   */
  constructor (src, isNew, vals, readOnly = false) {
    this.isNew = !!isNew
    this.__readOnly = readOnly
    if (!ITEM_SOURCES.has(src)) {
      throw new InvalidParameterError('src', 'invalid item source type')
    }
    this.__src = src

    // track whether this item has been written to the db yet
    this.__written = false

    // track whether this item has been marked for deletion
    this.__toBeDeleted = src.isDelete

    // __cached_attrs has a __Field subclass object for each non-key attribute.
    this.__cached_attrs = {}

    // __cached_attrs has a __Field subclass object for each non-key attribute.
    this.__attr_getters = {}

    // Decode _id and _sk that are stored in DB into key components that are
    // in KEY and SORT_KEY.
    const setupKey = (attrName, keySchema, keyOrder, vals) => {
      const attrVal = vals[attrName]
      if (attrVal === undefined) {
        return
      }

      delete vals[attrName]
      const useNumericKey = this.constructor.__useNumericKey(keySchema)
      Object.assign(vals, this.constructor.__decodeCompoundValue(
        keyOrder, attrVal, attrName, useNumericKey))
    }
    setupKey('_id', this.constructor.KEY,
      this.constructor.__keyOrder.partition, vals)
    setupKey('_sk', this.constructor.SORT_KEY,
      this.constructor.__keyOrder.sort, vals)

    // add user-defined fields from FIELDS & key components from KEY & SORT_KEY
    let fieldIdx = 0
    for (const [name, opts] of Object.entries(this.constructor._attrs)) {
      this.__addField(fieldIdx++, name, opts, vals)
    }

    for (let field of this.constructor.__compoundFields) {
      if (typeof (field) === 'string') {
        field = [field]
      }
      this.__addCompoundField(fieldIdx++, field, isNew)
    }

    Object.seal(this)
  }

  static register (registrator) {
    this.__doOneTimeModelPrep()
    registrator.registerModel(this)
  }

  /**
   * Hook for finalizing a model before writing to database
   */
  finalize () {
  }

  __addField (idx, name, opts, vals) {
    let valSpecified = Object.hasOwnProperty.call(vals, name)
    let val = vals[name]
    if (!valSpecified && this.__readOnly) {
      /*
        If the model is a read-only (aka derived using index) and column val
        isn't specified, let's mark the field optional
      */
      opts.optional = true
      for (const [encodedName, encodedVal] of Object.entries(vals)) {
        const fieldData = __CompoundField.__decodeValues(encodedName, encodedVal)
        if (Object.hasOwnProperty.call(fieldData, name)) {
          valSpecified = true
          val = fieldData[name]
          break
        }
      }
    }
    const getCachedField = () => {
      if (this.__cached_attrs[name]) {
        return this.__cached_attrs[name]
      }
      const Cls = SCHEMA_TYPE_TO_FIELD_CLASS_MAP[opts.schema.type]
      // can't force validation of undefined values for blind updates because
      //   they are permitted to omit fields
      const field = new Cls({
        idx,
        name,
        opts,
        val,
        valIsFromDB: !this.isNew,
        valSpecified,
        isForUpdate: this.__src.isUpdate,
        isForDelete: this.__src.isDelete
      })
      Object.seal(field)
      this.__cached_attrs[name] = field
      return field
    }
    this.__attr_getters[name] = getCachedField
    if (this.isNew) {
      getCachedField() // create the field now to trigger validation
    }
    Object.defineProperty(this, name, {
      get: (...args) => {
        const field = getCachedField()
        return field.get()
      },
      set: (val) => {
        if (this.__readOnly) {
          throw new InvalidFieldError('', 'Can not modify a read-only model')
        }
        const field = getCachedField()
        field.set(val)
      }
    })
  }

  __addCompoundField (idx, fieldNames, isNew) {
    const name = this.constructor.__encodeCompoundFieldName(fieldNames)
    if (this.__attr_getters[name] !== undefined || ['_id', '_sk'].includes(name)) {
      return
    }
    const fields = fieldNames.map(field => this.__attr_getters[field]())
    const getCachedField = () => {
      if (this.__cached_attrs[name]) {
        return this.__cached_attrs[name]
      }
      const field = new __CompoundField({ idx, name, isNew, fields })
      this.__cached_attrs[name] = field
      return field
    }
    this.__attr_getters[name] = getCachedField
    getCachedField()
    Object.defineProperty(this, name, {
      get: (...args) => {
        const field = getCachedField()
        return field.get()
      },
      set: (val) => {
        const field = getCachedField()
        field.set(val)
      }
    })
  }

  static __getFields () {
    return this.FIELDS
  }

  static __validatedSchema () {
    if (Object.constructor.hasOwnProperty.call(this, '__CACHED_SCHEMA')) {
      return this.__CACHED_SCHEMA
    }

    if (!this.KEY) {
      throw new InvalidFieldError('KEY', 'the partition key is required')
    }
    if (this.KEY.isTodeaSchema || this.KEY.schema) {
      throw new InvalidFieldError('KEY', 'must define key component name(s)')
    }
    if (Object.keys(this.KEY).length === 0) {
      throw new InvalidFieldError('KEY', '/at least one partition key field/')
    }
    if (this.SORT_KEY?.isTodeaSchema || this.SORT_KEY?.schema) {
      throw new InvalidFieldError('SORT_KEY',
        'must define key component name(s)')
    }

    // cannot use the names of non-static Model members (only need to list
    // those that are defined by the constructor; those which are on the
    // prototype are enforced automatically)
    const reservedNames = new Set(['isNew'])
    const proto = this.prototype
    const ret = {}
    for (const schema of [this.KEY, this.SORT_KEY ?? {}, this.__getFields()]) {
      for (const [key, val] of Object.entries(schema)) {
        if (ret[key]) {
          throw new InvalidFieldError(
            key, 'property name cannot be used more than once')
        }
        if (reservedNames.has(key)) {
          throw new InvalidFieldError(
            key, 'field name is reserved and may not be used')
        }
        if (key in proto) {
          throw new InvalidFieldError(key, 'shadows a property name')
        }
        ret[key] = val
      }
    }

    for (const [index, keys] of Object.entries(this.INDEXES)) {
      if (keys.KEY === undefined) {
        throw new InvalidIndexError(index, 'partition key is required')
      }
      const indexFields = new Set()
      for (const field of [...keys.KEY, ...(keys.SORT_KEY ?? [])]) {
        if (field in ret === false) {
          throw new InvalidIndexError(index,
            'all field names must exist in the table')
        }
        if (indexFields.has(field)) {
          throw new InvalidIndexError(index,
            'field name cannot be used more than once')
        }
        indexFields.add(field)
      }
    }

    if (this.EXPIRE_EPOCH_FIELD) {
      const todeaSchema = ret[this.EXPIRE_EPOCH_FIELD]
      if (!todeaSchema) {
        throw new GenericModelError(
          'EXPIRE_EPOCH_FIELD must refer to an existing field',
          this.name
        )
      }
      const schema = todeaSchema.jsonSchema()
      if (!['integer', 'number'].includes(schema.type)) {
        throw new GenericModelError(
          'EXPIRE_EPOCH_FIELD must refer to an integer or double field',
          this.name
        )
      }
    }
    this.__CACHED_SCHEMA = S.obj(ret)
    return this.__CACHED_SCHEMA
  }

  static get schema () {
    return this.__validatedSchema()
  }

  static get __keyOrder () {
    if (Object.constructor.hasOwnProperty.call(this, '__CACHED_KEY_ORDER')) {
      return this.__CACHED_KEY_ORDER
    }
    this.__validatedSchema() // use side effect to validate schema
    this.__CACHED_KEY_ORDER = {
      partition: Object.keys(this.KEY).sort(),
      sort: Object.keys(this.SORT_KEY || {}).sort()
    }
    return this.__CACHED_KEY_ORDER
  }

  static __validateIndexKeys (index, data) {
    const isFieldOptional = (fields) => {
      if (fields.some(field => this._attrs[field].optional)) {
        return true
      }
      return false
    }
    if (!data.SPARSE) {
      if (isFieldOptional(data.KEY) || (data.SORT_KEY && isFieldOptional(data.SORT_KEY))) {
        throw new InvalidIndexError(index, `Can not use optional fields as key.
        Make it a sparse index to use optional fields`)
      }
    }
    this.__compoundFields.add(data.KEY)
    if (data.SORT_KEY) {
      this.__compoundFields.add(data.SORT_KEY)
    }

    if (data.INCLUDE_ONLY) {
      for (const field of data.INCLUDE_ONLY) {
        if (!this._attrs[field]) {
          throw new InvalidIndexError(index, `Field ${field} doesn't exist in the model`)
        }
        if (this.__keyOrder.partition.includes(field) || this.__keyOrder.sort.includes(field)) {
          throw new InvalidIndexError(index, `Field ${field} is a key attribute and is automatically included`)
        }
      }
    }
  }

  static __validateTableName () {
    const tableName = this.tableName
    try {
      assert.ok(!tableName.endsWith('Model'), 'not include "Model"')
      assert.ok(!tableName.endsWith('Table'), 'not include "Table"')
      assert.ok(tableName.indexOf('_') < 0, 'not include underscores')
      assert.ok(tableName[0].match(/[A-Z]/), 'start with a capitalized letter')
      assert.ok(tableName.match(/[a-zA-Z0-9]*/), 'only use letters or numbers')
      // we will eventually need to allow this in some cases since some
      // singular words also end in the letter "s"
      if (tableName.endsWith('s')) {
        const whiteList = [
          'Stats',
          'Status',
          'Pass'
        ]
        const isWhiteListed = whiteList.some(p => tableName.endsWith(p))
        assert.ok(isWhiteListed, 'not be plural')
      }
    } catch (e) {
      throw new Error(`Bad table name "${tableName}": it must ${e.message}`)
    }
  }

  /**
   * Check that field names don't overlap, etc.
   */
  static __doOneTimeModelPrep () {
    // need to check hasOwnProperty because we don't want to access this
    // property via inheritance (i.e., our parent may have been setup, but
    // the subclass must do its own setup)
    if (Object.hasOwnProperty.call(this, '__setupDone')) {
      return // one-time setup already done
    }
    this.__setupDone = true

    this.__validateTableName()
    // _attrs maps the name of attributes that are visible to users of
    // this model. This is the combination of attributes (keys) defined by KEY,
    // SORT_KEY and FIELDS.
    this._attrs = {}
    this.__compoundFields = new Set()
    this.__KEY_COMPONENT_NAMES = new Set()
    const partitionKeys = new Set(this.__keyOrder.partition)
    const sortKeys = new Set(this.__keyOrder.sort)
    for (const [fieldName, schema] of Object.entries(this.schema.objectSchemas)) {
      let keyType
      if (partitionKeys.has(fieldName)) {
        keyType = 'PARTITION'
      } else if (sortKeys.has(fieldName)) {
        keyType = 'SORT'
      }
      const finalFieldOpts = __Field.__validateFieldOptions(
        this.name, keyType || undefined, fieldName, schema)
      this._attrs[fieldName] = finalFieldOpts
      if (keyType) {
        this.__KEY_COMPONENT_NAMES.add(fieldName)
      }
    }
    if (this.INDEX_INCLUDE_KEYS) {
      this.__compoundFields = new Set(
        [...Object.keys(this.KEY), ...Object.keys(this.SORT_KEY)])
    }
    for (const [index, keys] of Object.entries(this.INDEXES)) {
      this.__validateIndexKeys(index, keys)
    }
  }

  static __useNumericKey (keySchema) {
    // ActionHistory table was the only table that had a numeric sort key
    // before we supported numeric sort keys. It is provisioned with a string
    // sort key, which happens to work, because the sort key is epoch, which
    // doesn't grow in magnitude in years, and therefore doesn't suffer from
    // ordering issue (when sorted, stringified numerical values are
    // incorrectly ordered, e.g. ["1", "10", "11", "2"]).
    // To properly fix this table, we will have to move ActionHistory logic to
    // point to a temporary table, delete the old table, and point logic back,
    // causing a service disruption for LR Admins who need to debug using
    // action history. However, since we are migrating ActionHistory from
    // DynamoDB to AWS TimeStream later, it makes more sense to hold off the
    // fix and wait for the migration (or the decision to not migrate) to
    // minimize negative impact.
    // TODO: Once we move ActionHistory table out of DynamoDB, we can remove
    // this hack.
    const isLegacyTable = ['ActionHistory'].includes(this.name)
    // istanbul ignore if
    if (isLegacyTable) {
      return false
    }
    const schemas = Object.values(keySchema)
    const isUniqueKey = schemas.length === 1
    if (!isUniqueKey) {
      return false
    }
    let schemaType
    if (typeof (schemas[0]) === 'string') {
      const classSchemas = { ...this.KEY, ...this.SORT_KEY, ...this.__getFields() }
      schemaType = classSchemas[schemas[0]].getProp('type')
    } else {
      schemaType = schemas[0].getProp('type')
    }
    const isNumericKey = ['integer', 'number'].includes(schemaType)
    return isNumericKey
  }

  static get resourceDefinitions () {
    this.__doOneTimeModelPrep()
    // the partition key attribute is always "_id"
    const keyType = this.__useNumericKey(this.KEY) ? 'N' : 'S'
    const attrs = [{ AttributeName: '_id', AttributeType: keyType }]
    const dedupeAttr = new Set(['_id'])
    const keys = [{ AttributeName: '_id', KeyType: 'HASH' }]
    const indexes = []

    // if we have a sort key attribute, it is always "_sk"
    if (this.__keyOrder.sort.length > 0) {
      const keyType = this.__useNumericKey(this.SORT_KEY) ? 'N' : 'S'
      attrs.push({ AttributeName: '_sk', AttributeType: keyType })
      keys.push({ AttributeName: '_sk', KeyType: 'RANGE' })
      dedupeAttr.add('_sk')
    }

    if (Object.keys(this.INDEXES).length > 0) {
      for (const [index, props] of Object.entries(this.INDEXES)) {
        const keyNames = this.__getKeyNamesForIndex(index)
        const indexProps = {
          IndexName: index,
          KeySchema: [{ AttributeName: keyNames._id, KeyType: 'HASH' }],
          Projection: { ProjectionType: 'ALL' },
          ...this.getProvisionedThroughputConfig()
        }
        if (props.INCLUDE_ONLY) {
          if (props.INCLUDE_ONLY.length === 0) {
            indexProps.Projection.ProjectionType = 'KEYS_ONLY'
          } else {
            indexProps.Projection = {
              ProjectionType: 'INCLUDE',
              NonKeyAttributes: props.INCLUDE_ONLY
            }
          }
        }
        if (dedupeAttr.has(keyNames._id) === false) {
          attrs.push({
            AttributeName: keyNames._id,
            AttributeType: this.__useNumericKey(props.KEY) ? 'N' : 'S'
          })
          dedupeAttr.add(keyNames._id)
        }

        if (props.SORT_KEY !== undefined) {
          indexProps.KeySchema.push({ AttributeName: keyNames._sk, KeyType: 'RANGE' })
          if (dedupeAttr.has(keyNames._sk) === false) {
            attrs.push({
              AttributeName: keyNames._sk,
              AttributeType: this.__useNumericKey(props.SORT_KEY) ? 'N' : 'S'
            })
            dedupeAttr.add(keyNames._sk)
          }
        }
        indexes.push(indexProps)
      }
    }

    const properties = {
      TableName: this.fullTableName,
      AttributeDefinitions: attrs,
      KeySchema: keys,
      BillingMode: {
        'Fn::If': [
          'IsProdServerCondition',
          'PROVISIONED',
          'PAY_PER_REQUEST'
        ]
      },
      ...this.getProvisionedThroughputConfig()
    }

    if (indexes.length > 0) {
      properties.GlobalSecondaryIndexes = indexes
    }

    if (this.EXPIRE_EPOCH_FIELD) {
      properties.TimeToLiveSpecification = {
        AttributeName: this.EXPIRE_EPOCH_FIELD,
        Enabled: true
      }
    }

    return {
      [this.tableResourceName]: {
        Type: 'AWS::DynamoDB::Table',
        DeletionPolicy: 'Retain',
        Properties: properties
      },
      ...this.getTableAutoScalingConfig(),
      ...this.getIndexesAutoScalingConfig(indexes)
    }
  }

  static getProvisionedThroughputConfig () {
    return {
      ProvisionedThroughput: {
        'Fn::If': [
          'IsProdServerCondition',
          {
            ReadCapacityUnits: 1,
            WriteCapacityUnits: 1
          },
          {
            Ref: 'AWS::NoValue'
          }
        ]
      }
    }
  }

  static getTableAutoScalingConfig () {
    const resourceId = `table/${this.fullTableName}`
    return this.getAutoScalingConfig(this.tableResourceName, resourceId, 'table')
  }

  static getIndexesAutoScalingConfig (indexes) {
    const indexesAutoScalingConfig = {}
    for (const index of indexes) {
      const indexName = index.IndexName
      const resourceId = `table/${this.fullTableName}/index/${indexName}`
      const indexResourceName = this.tableResourceName + `${indexName[0].toUpperCase()}${indexName.slice(1)}` + 'Index'
      const config = this.getAutoScalingConfig(indexResourceName, resourceId, 'index')
      Object.assign(indexesAutoScalingConfig, config)
    }
    return indexesAutoScalingConfig
  }

  static getAutoScalingConfig (resourceName, resourceId, dimension) {
    const readPolicyName = resourceName + 'ReadScalingPolicy'
    const readTargetName = resourceName + 'ReadScalableTarget'
    const writePolicyName = resourceName + 'WriteScalingPolicy'
    const writeTargetName = resourceName + 'WriteScalableTarget'
    return {
      [readPolicyName]: {
        Type: 'AWS::ApplicationAutoScaling::ScalingPolicy',
        Condition: 'IsProdServerCondition',
        Properties: {
          PolicyName: readPolicyName,
          PolicyType: 'TargetTrackingScaling',
          ScalingTargetId: {
            Ref: readTargetName
          },
          TargetTrackingScalingPolicyConfiguration: {
            TargetValue: 75,
            ScaleInCooldown: 0,
            ScaleOutCooldown: 0,
            PredefinedMetricSpecification: {
              PredefinedMetricType: 'DynamoDBReadCapacityUtilization'
            }
          }
        }
      },
      [readTargetName]: {
        Type: 'AWS::ApplicationAutoScaling::ScalableTarget',
        Condition: 'IsProdServerCondition',
        DependsOn: this.tableResourceName,
        Properties: {
          MaxCapacity: 1000,
          MinCapacity: 1,
          ResourceId: resourceId,
          RoleARN: {
            'Fn::Sub': 'arn:aws:iam::${AWS::AccountId}:role/aws-service-role/dynamodb.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_DynamoDBTable' // eslint-disable-line no-template-curly-in-string
          },
          ScalableDimension: `dynamodb:${dimension}:ReadCapacityUnits`,
          ServiceNamespace: 'dynamodb'
        }
      },
      [writePolicyName]: {
        Type: 'AWS::ApplicationAutoScaling::ScalingPolicy',
        Condition: 'IsProdServerCondition',
        Properties: {
          PolicyName: writePolicyName,
          PolicyType: 'TargetTrackingScaling',
          ScalingTargetId: {
            Ref: writeTargetName
          },
          TargetTrackingScalingPolicyConfiguration: {
            TargetValue: 75,
            ScaleInCooldown: 0,
            ScaleOutCooldown: 0,
            PredefinedMetricSpecification: {
              PredefinedMetricType: 'DynamoDBWriteCapacityUtilization'
            }
          }
        }
      },
      [writeTargetName]: {
        Type: 'AWS::ApplicationAutoScaling::ScalableTarget',
        Condition: 'IsProdServerCondition',
        DependsOn: this.tableResourceName,
        Properties: {
          MaxCapacity: 1000,
          MinCapacity: 1,
          ResourceId: resourceId,
          RoleARN: {
            'Fn::Sub': 'arn:aws:iam::${AWS::AccountId}:role/aws-service-role/dynamodb.application-autoscaling.amazonaws.com/AWSServiceRoleForApplicationAutoScaling_DynamoDBTable' // eslint-disable-line no-template-curly-in-string
          },
          ScalableDimension: `dynamodb:${dimension}:WriteCapacityUnits`,
          ServiceNamespace: 'dynamodb'
        }
      }
    }
  }

  /**
   * Defines the partition key. Every item in the database is uniquely
   * identified by the combination of its partition and sort key. The default
   * partition key is a UUIDv4.
   *
   * A key can simply be some scalar value:
   *   static KEY = { id: S.str }
   *
   * A key may can be "compound key", i.e., a key with one or components, each
   * with their own name and schema:
   *   static KEY = {
   *     email: S.str,
   *     birthYear: S.int.min(1900)
   *   }
   */
  static KEY = { id: S.SCHEMAS.UUID }

  /** Defines the sort key, if any. Uses the compound key format from KEY. */
  static SORT_KEY = {}

  /**
   * Defines the non-key fields. By default there are no fields.
   *
   * Properties are defined as a map from field names to a Todea schema:
   * @example
   *   static FIELDS = {
   *     someNumber: S.double,
   *     someNumberWithOptions: S.double.optional().default(0).readOnly()
   *   }
   */
  static FIELDS = {}

  /**
   * Defines the global secondary indexes for the table.
   * By default there are no secondary indexes.
   *
   * Properties are defined as a map from index names to an object
   * containing the keys for the index
   * (fields defined in an index key should already exist in the table):
   * @example
   *   static INDEXES = {
   *     index1: { KEY: [field1, field2] }
   *     index2: { KEY: [field3], SORT_KEY: [field4] }
   *   }
   */
  static INDEXES = {}

  /**
   * If this is enabled, we will create individual fields internally for
   * all the field in KEY, SORT_KEY. This will incrase the storage cost but
   * enable users to use lazy filter on individual key fields.
   * This works only for querying via indexes, if the key field isn't part
   * of a key in that index.
   */
  static INDEX_INCLUDE_KEYS = false

  get _id () {
    return this.__getKey(this.constructor.__keyOrder.partition,
      this.constructor.KEY)
  }

  get _sk () {
    return this.__getKey(this.constructor.__keyOrder.sort,
      this.constructor.SORT_KEY)
  }

  __getKey (keyOrder, keySchema) {
    const useNumericKey = this.constructor.__useNumericKey(keySchema)
    return this.constructor.__encodeCompoundValue(
      keyOrder,
      new Proxy(this, {
        get: (target, prop, receiver) => {
          return target.getField(prop).__value
        }
      }),
      useNumericKey
    )
  }

  get __encodedKey () {
    const ret = {
      _id: this._id
    }
    const sk = this._sk
    if (sk !== undefined) {
      ret._sk = sk
    }
    return ret
  }

  static __getId (vals) {
    const useNumericKey = this.__useNumericKey(this.KEY)
    return this.__encodeCompoundValue(this.__keyOrder.partition, vals, useNumericKey)
  }

  static __getSk (vals) {
    if (this.__keyOrder.sort.length <= 0) {
      return undefined
    }
    const useNumericKey = this.__useNumericKey(this.SORT_KEY)
    return this.__encodeCompoundValue(this.__keyOrder.sort, vals, useNumericKey)
  }

  static __getIndexCompoundValue (keys, vals) {
    if (keys.length === 1 && ['string', 'number'].includes(typeof vals[keys[0]])) {
      return vals[keys[0]]
    }
    const fieldName = this.__encodeCompoundFieldName(keys)
    if (['_id', '_sk'].includes(fieldName)) {
      // using model key encoding so use existing logic
      return this.__encodeCompoundValue(keys, vals, this.__useNumericKey(keys))
    }
    return __CompoundField.__encodeValues(keys, vals)
  }

  static __getIdForIndex (vals, index) {
    const keys = this.INDEXES[index].KEY.sort()
    return this.__getIndexCompoundValue(keys, vals)
  }

  static __getSkForIndex (vals, index) {
    const keys = this.INDEXES[index].SORT_KEY.sort()
    return this.__getIndexCompoundValue(keys, vals)
  }

  /**
   * Generate a compound field name given a list of fields.
   * For compound field containing a single field that is not either a PK or SK,
   * we use the same name as the original field to reduce data duplication.
   * We also auto-detect if _id, or _sk can be re-used
   *
   * @param [ fields ] a list of string denoting the fields
   * @returns a string denoting the compound field's internal name
   */
  static __encodeCompoundFieldName (fields) {
    if (fields.length === 1 && this.FIELDS[fields[0]] &&
      !['array', 'object', 'boolean'].includes(this.FIELDS[fields[0]].getProp('type'))) {
      return fields[0]
    }

    if (Object.keys(this.KEY).sort().join('\0') === fields.sort().join('\0')) {
      return '_id'
    }
    if (Object.keys(this.SORT_KEY).sort().join('\0') === fields.sort().join('\0')) {
      return '_sk'
    }

    return __CompoundField.__encodeName(fields)
  }

  static __getKeyNamesForIndex (index) {
    return {
      _id: this.__encodeCompoundFieldName(this.INDEXES[index].KEY),
      _sk: this.__encodeCompoundFieldName(this.INDEXES[index].SORT_KEY ?? [])
    }
  }

  /**
   * Returns a map containing the model's computed key values (_id, as well as
   * _sk if model has a sort key).
   * Verifies that the keys are valid (i.e., they match the required schema).
   * @param {Object} vals map of field names to values
   * @returns map of _id (and _sk attribute values)
   */
  static __computeKeyAttrMap (vals) {
    // compute and validate the partition attribute
    const ret = { _id: this.__getId(vals) }
    const sk = this.__getSk(vals)
    if (sk !== undefined) { // Account for 0
      ret._sk = sk
    }
    return ret
  }

  /**
   * Returns the underlying __Field associated with an attribute.
   *
   * @param {String} name the name of a field from FIELDS
   * @returns {BooleanField|ArrayField|ObjectField|NumberField|StringField}
   */
  getField (name) {
    assert(!name.startsWith('_'), 'may not access internal computed fields')
    return this.__attr_getters[name]()
  }

  /**
   * The table name this model is associated with, excluding the service ID
   * prefix. This is the model's class name. However, subclasses may choose to
   * override this method and provide duplicated table name for co-existed
   * models.
   *
   * @type {String}
   */
  static get tableName () {
    return this.name
  }

  /**
   * Returns the fully-qualified table name (Service ID + tableName).
   * @private
   */
  static get fullTableName () {
    return process.env.SERVICE + this.tableName
  }

  static get tableResourceName () {
    return 'DynamoDBTable' + this.fullTableName
  }

  /**
   * The table name this model is associated with.
   * Just a convenience wrapper around the static version of this method.
   * @private
   */
  get __fullTableName () {
    return Object.getPrototypeOf(this).constructor.fullTableName
  }

  /**
   * Given a mapping, split compositeKeys from other model fields. Return a
   * 3-tuple, [encodedKeys, keyComponents, modelData].
   *
   * @param {Object} data data to be split
   */
  static __splitKeysAndData (data) {
    const keyComponents = {}
    const modelData = {}
    Object.keys(data).forEach(key => {
      if (this.__KEY_COMPONENT_NAMES.has(key)) {
        keyComponents[key] = data[key]
      } else if (this._attrs[key]) {
        modelData[key] = data[key]
      } else {
        throw new InvalidParameterError('data', 'unknown field ' + key)
      }
    })
    return [this.__computeKeyAttrMap(keyComponents), keyComponents, modelData]
  }

  /**
   * @access package
   * @param {CompositeID} encodedKeys
   * @param {GetParams} options
   * @returns {Object} parameters for a get request to DynamoDB
   */
  static __getParams (encodedKeys, options) {
    return {
      TableName: this.fullTableName,
      ConsistentRead: !options.inconsistentRead,
      Key: encodedKeys
    }
  }

  /**
   * Parameters for fetching a model and options to control how a model is
   * fetched from database.
   * @typedef {Object} GetParams
   * @property {Boolean} [inconsistentRead=false] If true, model is read with
   *   strong consistency, else the read is eventually consistent.
   * @property {Boolean} [createIfMissing=false] If true, a model is returned
   *   regardless of whether the model exists on server. This behavior is the
   *   same as calling create when get(..., { createIfMissing: false }) returns
   *   undefined
   * @property {*} [*] Besides the predefined options, custom key-value pairs
   *   can be added. These values will be made available to the Model's
   *   constructor as an argument.
   */

  /**
   * Generates parameters for a put request to DynamoDB.
   * Put overrides item entirely, removing untracked fields from DynamoDB.
   * This library supports optimistic locking for put. Since put overrides all
   * fields of an item, optimistic locking is performed on all fields. This
   * means if any fields is modified after the item is read calling put would
   * fail. Effectively the lock applies to the entire item, which may lead to
   * more contention. Have update in most use cases is more desirable.
   *
   * @access package
   * @returns parameters for a put request to DynamoDB
   */
  __putParams () {
    // istanbul ignore next
    if (this.__src.isUpdate) {
      // This is really unreachable code.
      // The only way to get here is when the model is mutated (to complete a
      // write) and has no field mutated (so PUT is used instead of UPDATE).
      // It can happen only when the model isNew.
      // However, when items are setup from updateItem method, we pretend the
      // items to be not new. Hence, the condition will never be satisfied.
      // conditions.push('attribute_exists(_id)')
      assert(false, 'This should be unreachable unless something is broken.')
    }

    const item = this.__encodedKey
    const accessedFields = []
    let exprCount = 0
    for (const [key, getter] of Object.entries(this.__attr_getters)) {
      const field = getter()
      field.validate()

      if (field.keyType) {
        continue
      }
      if (field.__value !== undefined) {
        // Not having undefined keys effectively removes them.
        // Also saves some bandwidth.
        item[key] = deepcopy(field.__value)
      }

      // Put works by overriding the entire item,
      // all fields needs to be written.
      // No need to check for field.accessed, pretend everything is accessed,
      // except for keys, since they don't change
      accessedFields.push(field)
    }

    let conditionExpr
    const exprAttrNames = {}
    const isCreateOrPut = this.__src.isCreateOrPut
    const exprValues = {}
    if (this.isNew) {
      if (isCreateOrPut) {
        const conditions = []
        for (const field of accessedFields) {
          const exprKey = `:_${exprCount++}`
          const [condition, vals] = field.__conditionExpression(exprKey)
          if (condition &&
            (!isCreateOrPut ||
             !condition.startsWith('attribute_not_exists'))) {
            conditions.push(condition)
            Object.assign(exprValues, vals)
            exprAttrNames[field.__awsName] = field.name
          }
        }
        conditionExpr = conditions.join(' AND ')

        if (conditionExpr.length !== 0) {
          const [cond, names, vals] = this.__nonexistentModelCondition()
          conditionExpr = `${cond} OR
            (${conditionExpr})`
          Object.assign(exprAttrNames, names)
          Object.assign(exprValues, vals)
        }
      } else {
        const [cond, names, vals] = this.__nonexistentModelCondition()
        conditionExpr = cond
        Object.assign(exprAttrNames, names)
        Object.assign(exprValues, vals)
      }
    } else {
      const conditions = [
        'attribute_exists(#_id)'
      ]
      exprAttrNames['#_id'] = '_id'
      for (const field of accessedFields) {
        const exprKey = `:_${exprCount++}`
        const [condition, vals] = field.__conditionExpression(exprKey)
        if (condition) {
          conditions.push(condition)
          Object.assign(exprValues, vals)
          exprAttrNames[field.__awsName] = field.name
        }
      }
      conditionExpr = conditions.join(' AND ')
    }

    const ret = {
      TableName: this.__fullTableName,
      Item: item
    }
    if (conditionExpr.length !== 0) {
      ret.ConditionExpression = conditionExpr
    }
    if (Object.keys(exprValues).length) {
      ret.ExpressionAttributeValues = exprValues
    }
    if (Object.keys(exprAttrNames).length) {
      ret.ExpressionAttributeNames = exprAttrNames
    }
    return ret
  }

  /**
   * Generates parameters for an update request to DynamoDB.
   * Update only overrides fields that got updated to a different value.
   * Untracked fields will not be removed from DynamoDB. This library supports
   * optimistic locking for update. Since update only touches specific fields
   * of an item, optimistic locking is only performed on fields accessed (read
   * or write). This locking mechanism results in less likely contentions,
   * hence is preferred over put.
   *
   * @access package
   * @param {Boolean} omitUpdates (default = false)
   * When True, generates only condition expressions for read values;
   * skipping update expressions, related Attribute Names/Values and schema validation,
   * with the expectation that any accessed value is either unmodified (and therefore valid)
   * or explicitly unchecked (written but not read).
   * @returns parameters for a update request to DynamoDB
   */
  __updateParams (omitUpdates = false) {
    const conditions = []
    const exprAttrNames = {}
    const exprValues = {}
    const itemKey = this.__encodedKey
    const sets = []
    const removes = []
    const accessedFields = []
    let exprCount = 0

    const isUpdate = this.__src.isUpdate
    for (const field of Object.values(this.__cached_attrs)) {
      if (field.keyType) {
        // keyparts are never updated and not explicitly represented in store
        continue
      }
      if (field.accessed) {
        accessedFields.push(field)
      }
      if (!field.__mayHaveMutated || omitUpdates) {
        continue
      }

      field.validate()

      const exprKey = `:_${exprCount++}`
      const [set, vals, remove] = field.__updateExpression(exprKey)
      if (set) {
        sets.push(set)
        Object.assign(exprValues, vals)
      }
      if (remove) {
        removes.push(field.__awsName)
      }
      if (set || remove) {
        exprAttrNames[field.__awsName] = field.name
      }
    }

    if (this.isNew) {
      if (!this.__src.isCreateOrPut) {
        const [cond, names, vals] = this.__nonexistentModelCondition()
        conditions.push(cond)
        Object.assign(exprAttrNames, names)
        Object.assign(exprValues, vals)
      }
    } else {
      conditions.push('attribute_exists(#_id)')
      exprAttrNames['#_id'] = '_id'

      for (const field of accessedFields) {
        const exprKey = `:_${exprCount++}`
        const [condition, vals] = field.__conditionExpression(exprKey)
        if (
          condition &&
          (!isUpdate || !condition.startsWith('attribute_not_exists'))
        ) {
          // From update, initial values for fields aren't setup.
          // We only care about the fields that got setup. Here if the
          // condition is attribute_not_exists, we know the field wasn't setup,
          // so ignore it.
          conditions.push(condition)
          Object.assign(exprValues, vals)
          exprAttrNames[field.__awsName] = field.name
        }
      }
    }

    const ret = {
      TableName: this.__fullTableName,
      Key: itemKey
    }
    const actions = []
    if (sets.length) {
      actions.push(`SET ${sets.join(',')}`)
    }
    if (removes.length) {
      actions.push(`REMOVE ${removes.join(',')}`)
    }
    if (actions.length) {
      // NOTE: This is optional in DynamoDB's update call,
      // but required in the transactWrite.update counterpart.
      ret.UpdateExpression = actions.join(' ')
    }
    if (conditions.length) {
      ret.ConditionExpression = conditions.join(' AND ')
    }
    if (Object.keys(exprValues).length) {
      ret.ExpressionAttributeValues = exprValues
    }
    // istanbul ignore else
    if (Object.keys(exprAttrNames).length) {
      ret.ExpressionAttributeNames = exprAttrNames
    }
    return ret
  }

  __deleteParams () {
    const itemKey = this.__encodedKey
    const ret = {
      TableName: this.__fullTableName,
      Key: itemKey
    }
    if (!this.isNew) {
      const conditions = []
      const attrNames = {}
      // Since model is not new, conditionCheckParams will always have contents
      const conditionCheckParams = this.__updateParams(true)
      conditions.push(conditionCheckParams.ConditionExpression)
      Object.assign(attrNames, conditionCheckParams.ExpressionAttributeNames)
      ret.ExpressionAttributeValues =
          conditionCheckParams.ExpressionAttributeValues

      ret.ConditionExpression = conditions.join(' AND ')
      ret.ExpressionAttributeNames = attrNames
    }
    return ret
  }

  /**
   * Indicates if any field was mutated. New models are considered to be
   * mutated as well.
   * @param {Boolean} expectWrites whether the model will be updated,
   *  default is true.
   * @type {Boolean}
   */
  __isMutated (expectWrites = true) {
    if (this.isNew) {
      return true
    }
    if (this.__toBeDeleted) {
      return true
    }
    for (const field of Object.values(this.__cached_attrs)) {
      if (field.hasChangesToCommit(expectWrites)) {
        // If any field has changes that need to be committed,
        // it will mark the model as mutated.
        return true
      }
    }
    return false
  }

  /**
   * Used for optimistic locking within transactWrite requests, when the model
   * was read in a transaction, and was subsequently used for updating other
   * models but never written back to DB. Having conditionCheck ensures this
   * model's data hasn't been changed so the updates to other models are also
   * correct.
   *
   * @access package
   * @returns {Boolean} An Object for ConditionCheck request.
   */
  __conditionCheckParams () {
    assert.ok(this.isNew || !this.__isMutated(),
      'Model is mutated, write it instead!')
    // Since model cannot be new, conditionCheckExpression will never be empty
    // (_id must exist)
    return this.__updateParams(true)
  }

  /**
   * Returns the string representation for the given compound values.
   *
   * This method throws {@link InvalidFieldError} if the compound value does
   * not match the required schema.
   *
   * @param {Array<String>} keyOrder order of keys in the string representation
   * @param {Object} values maps component names to values; may have extra
   *   fields (they will be ignored)
   */
  static __encodeCompoundValue (keyOrder, values, useNumericKey) {
    if (keyOrder.length === 0) {
      return undefined
    }

    const pieces = []
    for (let i = 0; i < keyOrder.length; i++) {
      const fieldName = keyOrder[i]
      const fieldOpts = this._attrs[fieldName]
      const givenValue = values[fieldName]
      if (givenValue === undefined) {
        throw new InvalidFieldError(fieldName, 'must be provided')
      }
      const valueType = validateValue(fieldName, fieldOpts, givenValue)
      if (useNumericKey) {
        return givenValue
      }
      if (valueType === String) {
        // the '\0' character cannot be stored in string fields. If you need to
        // store a string containing this character, then you need to store it
        // inside of an object field, e.g.,
        // item.someObjField = { myString: '\0' } is okay
        if (givenValue.indexOf('\0') !== -1) {
          throw new InvalidFieldError(
            fieldName, 'cannot put null bytes in strings in compound values')
        }
        pieces.push(givenValue)
      } else {
        pieces.push(stableStringify(givenValue))
      }
    }
    return pieces.join('\0')
  }

  /**
   * Returns the map which corresponds to the given compound value string
   *
   * This method throws {@link InvalidFieldError} if the decoded string does
   * not match the required schema.
   *
   * @param {Array<String>} keyOrder order of keys in the string representation
   * @param {String} strVal the string representation of a compound value
   * @param {String} attrName which key we're parsing
   */
  static __decodeCompoundValue (keyOrder, val, attrName, useNumericKey) {
    if (useNumericKey) {
      const fieldName = keyOrder[0]
      const fieldOpts = this._attrs[fieldName]
      validateValue(fieldName, fieldOpts, val)
      return { [fieldName]: val }
    }

    // Assume val is otherwise a string
    const pieces = val.split('\0')
    if (pieces.length !== keyOrder.length) {
      throw new InvalidFieldError(
        attrName, 'failed to parse key: incorrect number of components')
    }

    const compoundID = {}
    for (let i = 0; i < pieces.length; i++) {
      const piece = pieces[i]
      const fieldName = keyOrder[i]
      const fieldOpts = this._attrs[fieldName]
      const valueType = SCHEMA_TYPE_TO_JS_TYPE_MAP[fieldOpts.schema.type]
      if (valueType === String) {
        compoundID[fieldName] = piece
      } else {
        compoundID[fieldName] = JSON.parse(piece)
      }

      validateValue(fieldName, fieldOpts, compoundID[fieldName])
    }
    return compoundID
  }

  /**
   * Returns a Key identifying a unique row in this model's DB table.
   * @param {*} vals map of key component names to values; if there is
   *   only one partition key field (whose type is not object), then this MAY
   *   instead be just that field's value.
   * @returns {Key} a Key object.
   */
  static key (vals) {
    const processedVals = this.__splitKeysAndDataWithPreprocessing(vals)
    const [encodedKeys, keyComponents, data] = processedVals

    // ensure that vals only contained key components (no data components)
    const dataKeys = Object.keys(data)
    if (dataKeys.length) {
      dataKeys.sort()
      throw new InvalidParameterError('vals',
        `received non-key fields: ${dataKeys.join(', ')}`)
    }
    return new Key(this, encodedKeys, keyComponents)
  }

  /**
   * Returns a Data fully describing a unique row in this model's DB table.
   * @param {*} vals like the argument to key() but also includes non-key data
   * @returns {Data} a Data object for use with tx.create() or
   *   tx.get(..., { createIfMissing: true })
   */
  static data (vals) {
    return new Data(this, ...this.__splitKeysAndDataWithPreprocessing(vals))
  }

  static __splitKeysAndDataWithPreprocessing (vals) {
    // if we only have one key component, then the `_id` **MAY** just be the
    // value rather than a map of key component names to values
    assert(this.__setupDone,
      `model ${this.name} one-time setup was not done (remember to export ` +
      'the model and in unit tests remember to call createResources()')
    const pKeyOrder = this.__keyOrder.partition
    if (pKeyOrder.length === 1 && this.__keyOrder.sort.length === 0) {
      const pFieldName = pKeyOrder[0]
      if (!(vals instanceof Object) || !vals[pFieldName]) {
        vals = { [pFieldName]: vals }
      }
    }
    if (!(vals instanceof Object)) {
      throw new InvalidParameterError('values',
        'should be an object mapping key component names to values')
    }
    return this.__splitKeysAndData(vals)
  }

  __markForDeletion () {
    if (this.__toBeDeleted) {
      throw new ModelDeletedTwiceError(this)
    }
    this.__toBeDeleted = true
  }

  __writeMethod () {
    if (this.__toBeDeleted) {
      return 'delete'
    }
    const usePut = this.__src.isCreateOrPut
    return usePut ? 'put' : 'update'
  }

  /**
   * Writes model to database. Uses DynamoDB update under the hood.
   * @access package
   */
  async __write () {
    assert.ok(!this.__written, 'May write once')
    this.__written = true

    const method = this.__writeMethod()
    const params = this[`__${method}Params`]()
    const retries = 3
    let millisBackOff = 40
    for (let tryCnt = 0; tryCnt <= retries; tryCnt++) {
      try {
        await this.documentClient[method](params).promise().catch(
          // istanbul ignore next
          e => { throw new AWSError('write model', e) }
        )
        return
      } catch (error) {
        if (!error.retryable) {
          const isConditionalCheckFailure =
            error.code === 'ConditionalCheckFailedException'
          if (isConditionalCheckFailure && this.__toBeDeleted) {
            throw new InvalidModelDeletionError(
              this.constructor.tableName, this._id, this._sk)
          } else if (isConditionalCheckFailure && this.__src.isCreate) {
            throw new ModelAlreadyExistsError(
              this.constructor.tableName, this._id, this._sk)
          } else if (isConditionalCheckFailure && this.__src.isUpdate) {
            throw new InvalidModelUpdateError(
              this.constructor.tableName, this._id, this._sk)
          } else {
            throw error
          }
        }
      }
      if (tryCnt >= retries) {
        throw new Error('Max retries reached')
      }
      const offset = Math.floor(Math.random() * millisBackOff * 0.2) -
        millisBackOff * 0.1 // +-0.1 backoff as jitter to spread out conflicts
      await sleep(millisBackOff + offset)
      millisBackOff *= 2
    }
  }

  /**
   * Checks if the model has expired due to TTL.
   *
   * @private
   * @return true if TTL is turned on for the model, and there is a expiration
   *   time set for the current model, and the expiration time is smaller than
   *   current time.
   */
  get __hasExpired () {
    if (!this.constructor.EXPIRE_EPOCH_FIELD) {
      return false
    }
    const expirationTime = this.getField(this.constructor.EXPIRE_EPOCH_FIELD)
      .__value
    if (!expirationTime) {
      return false
    }
    const currentSecond = Math.ceil(new Date().getTime() / 1000)
    // When TTL is more than 5 years in the past, TTL is disabled
    // https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/
    // time-to-live-ttl-before-you-start.html
    const lowerBound = currentSecond - 157680000
    return expirationTime > lowerBound && expirationTime <= currentSecond
  }

  /**
   * @return a [ConditionExpression, ExpressionAttributeNames,
   *   ExpressionAttributeValues] tuple to make sure the model
   *   does not exist on server.
   */
  __nonexistentModelCondition () {
    let condition = 'attribute_not_exists(#_id)'
    const attrNames = {
      '#_id': '_id'
    }
    let attrValues
    if (this.constructor.EXPIRE_EPOCH_FIELD) {
      const expireField = this.getField(this.constructor.EXPIRE_EPOCH_FIELD)
      const currentSecond = Math.ceil(new Date().getTime() / 1000)

      // When TTL is more than 5 years in the past, TTL is disabled
      // https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/
      // time-to-live-ttl-before-you-start.html
      const lowerBound = currentSecond - 157680000
      const awsName = expireField.__awsName
      condition = `(${condition} OR
        (attribute_exists(${awsName}) AND
         :_ttlMin <= ${awsName} AND
         ${awsName} <= :_ttlMax))`
      attrNames[awsName] = expireField.name
      attrValues = {
        ':_ttlMin': lowerBound,
        ':_ttlMax': currentSecond
      }
    }

    return [
      condition,
      attrNames,
      attrValues
    ]
  }

  /**
   * Must be the same as NonExistentModel.toString() because it is used as the
   * unique identifier of an item for Objects and Sets.
   */
  toString () {
    return makeItemString(
      this.constructor,
      this._id,
      this._sk
    )
  }

  toJSON () {
    return this.getSnapshot()
  }

  /**
   * Return snapshot of the model, all fields included.
   * @param {Object} params
   * @param {Boolean} params.initial Whether to return the initial state
   * @param {Boolean} params.dbKeys Whether to return _id and _sk instead of
   *   raw key fields.
   */
  getSnapshot ({ initial = false, dbKeys = false } = {}) {
    if (initial === false && this.__toBeDeleted) {
      return undefined
    }

    const ret = {}
    if (dbKeys) {
      if (!initial || !this.isNew) {
        Object.assign(ret, this.__encodedKey)
      } else {
        ret._id = undefined
        if (this._sk) {
          ret._sk = undefined
        }
      }
    }
    for (const [name, getter] of Object.entries(this.__attr_getters)) {
      const field = getter()
      if (!field || field instanceof __CompoundField) {
        continue
      }
      if (field.keyType) {
        if (dbKeys) {
          continue
        }
      }
      if (initial) {
        ret[name] = field.__initialValue
      } else {
        ret[name] = field.__value
      }
    }
    return ret
  }

  static async describeTable () {
    try {
      const data = await this.dbClient.describeTable({
        TableName: this.fullTableName
      }).promise()
      const { ItemCount: itemCount, TableSizeBytes: sizeInBytes } = data.Table
      return { itemCount, sizeInBytes }
    } catch (e) {
      // istanbul ignore next
      throw new AWSError('describeTable', e)
    }
  }
}

/**
 * Used for tracking a non-existent item.
 */
class NonExistentItem {
  constructor (key) {
    this.key = key
  }

  get __src () {
    return {
      isGet: true
    }
  }

  get _id () {
    return this.key.encodedKeys._id
  }

  get _sk () {
    return this.key.encodedKeys._sk
  }

  get __fullTableName () {
    return this.key.Cls.fullTableName
  }

  __isMutated () {
    return false
  }

  __conditionCheckParams () {
    const condition = 'attribute_not_exists(#_id)'
    const attrNames = {
      '#_id': '_id'
    }
    return {
      TableName: this.key.Cls.fullTableName,
      Key: this.key.encodedKeys,
      ConditionExpression: condition,
      ExpressionAttributeNames: attrNames
    }
  }

  /**
   * Must be the same as Model.toString() because it is used as the unique
   * identifier of an item for Objects and Sets.
   */
  toString () {
    return makeItemString(
      this.key.Cls, this.key.encodedKeys._id, this.key.encodedKeys._sk)
  }

  getSnapshot () {
    return undefined
  }
}

module.exports = {
  Model,
  NonExistentItem
}
