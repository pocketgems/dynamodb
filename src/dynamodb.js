const ajv = new (require('ajv'))({
  allErrors: true,
  removeAdditional: 'failing'
})
const assert = require('assert')
const deepeq = require('deep-equal')
const deepcopy = require('rfdc')()
const S = require('fluent-schema')

/**
 * @namespace Errors
 */

/**
 * Thrown when supplied option is invalid.
 *
 * @access public
 * @memberof Errors
 */
class InvalidOptionsError extends Error {
  constructor (option, expectation) {
    super(`Invalid option value for ${option}. ${expectation}.`)
    this.name = this.constructor.name
  }
}

/**
 * Thrown when some parameter is invalid.
 *
 * @access public
 * @memberof Errors
 */
class InvalidParameterError extends Error {
  constructor (param, expectation) {
    super(`Invalid parameter ${param}. ${expectation}.`)
    this.name = this.constructor.name
  }
}

/**
 * Thrown when the library detects a field to be in an invalid state.
 *
 * @access public
 * @memberof Errors
 */
class InvalidFieldError extends Error {
  constructor (field, reason) {
    super(`${field || ''} ${reason}`)
    this.name = this.constructor.name
  }
}

/**
 * Thrown when a transaction fails.
 * Original exception is attached to property `original`
 * Original stack is appended to current stack.
 *
 * @access public
 * @memberof Errors
 */
class TransactionFailedError extends Error {
  constructor (obj) {
    super(obj)
    this.name = this.constructor.name
    this.original = obj
    if (obj instanceof Error) {
      this.stack += '\n' + obj.stack
    }
  }
}

/**
 * Thrown when a model is to be created, but DB already has an item with the
 * same key.
 */
class ModelAlreadyExistsError extends Error {
  constructor (_id, _sk) {
    const skStr = (_sk !== undefined) ? ` _sk=${_sk}` : ''
    super(`Tried to recreate an existing model: _id=${_id}${skStr}`)
    this.name = this.constructor.name
  }
}

async function sleep (millis) {
  return new Promise((resolve, reject) => {
    setTimeout(resolve, millis)
  })
}

function checkUnexpectedOptions (options, defaults) {
  if (typeof options !== 'object') {
    throw new InvalidParameterError('options', 'must be an object')
  }
  Object.keys(options).forEach(opt => {
    if (!Object.prototype.hasOwnProperty.call(defaults, opt)) {
      throw new InvalidOptionsError(opt, 'Unexpected option. ' +
        `Valid options are ${Object.keys(defaults)}`)
    }
    const optionVal = options[opt]
    const defaultVal = defaults[opt]
    if (optionVal !== undefined &&
        defaultVal !== undefined &&
        typeof optionVal !== typeof defaultVal) {
      throw new InvalidOptionsError(opt, 'Unexpected option. ' +
        `Invalid type for option ${opt}. Expected ${typeof defaultVal}`)
    }
  })
}

function loadOptionDefaults (options, defaults) {
  options = options || {}
  checkUnexpectedOptions(options, defaults)
  const retOptions = Object.assign({}, defaults)
  return Object.assign(retOptions, options)
}

/**
 * @namespace Fields
 * @memberof Internal
 */

/**
 * @namespace Internal
 */

/**
 * Internal object representing a field / property of a Model.
 *
 * @private
 * @memberof Internal
 */
class __Field {
  static __validateFieldOptions (keyType, fieldName, schema) {
    if (fieldName.startsWith('_')) {
      if (fieldName !== '_sk' && fieldName !== '_id') {
        throw new InvalidFieldError(
          fieldName, 'property names may not start with "_"')
      }
    }

    assert.ok(schema.isFluentSchema, 'should be fluent-schema')
    schema = schema.valueOf()
    const isKey = !!keyType
    const options = {
      keyType,
      schema,
      optional: schema.required === undefined,
      immutable: isKey || schema.readOnly === true,
      default: schema.default
    }
    if (options.schema.type !== 'object' && options.schema.required) {
      delete options.schema.required
    }
    const FieldCls = schemaTypeToFieldClassMap[options.schema.type]
    if (!FieldCls) {
      throw new InvalidFieldError(
        fieldName, `unsupported field type ${options.schema.type}`)
    }

    const hasDefault = Object.prototype.hasOwnProperty.call(schema, 'default')
    if (hasDefault && options.default === undefined) {
      throw new InvalidFieldError(fieldName,
        'the default value cannot be set to undefined')
    }
    if (isKey) {
      if (hasDefault) {
        throw new InvalidOptionsError('default',
          'No defaults for keys. It just doesn\'t make sense.')
      }
      if (schema.readOnly === false) {
        throw new InvalidOptionsError('immutable',
          'Keys must be immutable.')
      }
      if (options.optional) {
        throw new InvalidOptionsError('optional',
          'Keys must never be optional.')
      }
    }
    options.schemaValidator = ajv.compile(options.schema)
    return options
  }

  /**
   * @typedef {Object} FieldOptions
   * @property {'HASH'|'RANGE'} [keyType=undefined] If specified, the field is
   *   a key. Use 'HASH' for a partition key. Use 'RANGE' for a sort key.
   *   When keyType is specified, other options are forced to be
   *   { optional: false, immutable: true, default: undefined }. If user
   *   supplied values that conflicts with those values, InvalidOptionsError
   *   will be thrown.
   * @property {Boolean} [optional=false] If field can be left undefined.
   * @property {Boolean} [immutable=false] If field can be changed again after
   *   value is set to anything except undefined.
   * @property {*} [default=undefined] Default value to use. IMPORTANT: Value
   *   is deeply copied, so additional modifications to the parameter will
   *   not reflect in the field.
   * @property {schema} [schema=undefined] An optional JSON schema
   *   to validate Field's value.
   */

  /**
   * @param {FieldOptions} [options]
   */
  constructor (options) {
    for (const [key, value] of Object.entries(options)) {
      Object.defineProperty(this, key, {
        value: (key === 'default') ? deepcopy(value) : value,
        writable: false
      })
    }

    // Setup states
    /**
     * @memberof Internal.__Field
     * @instance
     * @member {String} name The name of the owning property.
     */
    this.name = undefined // Will be set after params for model are setup
    this.__initialValue = undefined
    this.__value = undefined
    this.__read = false // If get is called
    this.__written = false // If set is called
    if (options.default !== undefined) {
      this.set(this.default)
      this.__written = false
    }
  }

  /**
   * Sets up field's state using data fetched from server. Seals the object to
   * prevent futher modifications.
   *
   * @access package
   */
  __setup (val) {
    // Val is from server. We don't store undefined on server: we remove the
    // key on write. So if val is undefined, server does not have value for it.
    // Then don't set __value to keep the default.
    if (val !== undefined) {
      // Copy for initial value so changes through __value doesn't affect it.
      this.__initialValue = deepcopy(val)
      this.__value = val
    }

    // Don't add or remove properties after initialization.
    Object.seal(this)
  }

  /**
   * Generates a [SET, AttributeValues, REMOVE] tuple.
   *
   * @access package
   * @param {String} exprKey A key to use to link values in ConditionExpression
   *   and ExpressionAttributeValues
   * @returns {Array} [ConditionExpression, ExpressionAttributeValues,
   *   ShouldRemove]
   */
  __updateExpression (exprKey) {
    if (this.mutated) {
      if (this.__value === undefined) {
        return [undefined, {}, true]
      } else {
        return [
          `${this.name}=${exprKey}`,
          { [exprKey]: deepcopy(this.__value) },
          false
        ]
      }
    }
    return []
  }

  /**
   * Generates a [ConditionExpression, ExpressionAttributeValues] pair.
   *
   * @access package
   * @param {String} exprKey A key to use to link values in ConditionExpression
   *   and ExpressionAttributeValues
   * @returns {Array} [ConditionExpression, ExpressionAttributeValues]
   */
  __conditionExpression (exprKey) {
    if (this.__initialValue === undefined) {
      return [
        `attribute_not_exists(${this.name})`,
        {}
      ]
    }
    return [
      `${this.name}=${exprKey}`,
      { [exprKey]: this.__initialValue }
    ]
  }

  /**
   * This method compares initialValue against the current value.
   *
   * @returns if value was changed.
   */
  get mutated () {
    return this.__value !== this.__initialValue
  }

  /**
   * This is primarily used for optimistic locking.
   * @returns {Boolean} if the field was accessed (read / write) by users of
   *   this library
   */
  get accessed () {
    return this.__read || this.__written
  }

  /**
   * Gets the field's current value. Calling this method will mark the field as
   * "{@link accessed}".
   *
   * @see {@link __value} for accessing value within the library without
   *   "accessing" the field
   * @access public
   */
  get () {
    this.__read = true
    return this.__value
  }

  /**
   * If the value passed in is valid, update field's current value, mark the
   * field as "{@link accessed}". If the value is not valid, throws
   * InvalidFieldError.
   *
   * @param {*} value New value for the field.
   * @affects {@link __Field#accessed}
   * @access public
   */
  set (val) {
    // If field is immutable
    // And it's been written or has a value
    if (this.immutable && this.__value !== undefined) {
      throw new InvalidFieldError(
        this.name,
        'is immutable so value cannot be changed after first initialized.')
    }

    const prev = [this.__value, this.__written]
    this.__value = val
    this.__written = true

    try {
      this.validate()
    } catch (e) {
      [this.__value, this.__written] = prev
      throw e
    }
  }

  /**
   * Checks if the field's current value is valid. Throws InvalidFieldError if
   * check fails.
   */
  validate () {
    validateValue(this.name, this, this.__value)
  }
}

function validateValue (fieldName, opts, val) {
  const schema = opts.schema
  const valueType = schemaTypeToJSTypeMap[schema.type]

  // handle omitted value
  if (val === undefined) {
    if (opts.optional) {
      return valueType
    } else {
      throw new InvalidFieldError(fieldName, 'missing required value')
    }
  }

  // make sure the value is of the correct type
  if (val.constructor.name !== valueType.name) {
    throw new InvalidFieldError(fieldName,
      `value ${val} is not type ${valueType.name}`)
  }

  // validate the value against the provided schema
  const validator = opts.schemaValidator
  if (!validator(val)) {
    throw new InvalidFieldError(
      fieldName,
      `value ${JSON.stringify(val)} does not conform to schema ` +
      `${JSON.stringify(schema)} with error ` +
      JSON.stringify(validator.errors, null, 2)
    )
  }
  return valueType
}

/**
 * @extends Internal.__Field
 * @memberof Internal.Fields
 * @private
 */
class NumberField extends __Field {
  constructor (options) {
    super(options)
    this.__diff = undefined
  }

  set (val) {
    if (this.__diff !== undefined) {
      throw new Error('May not mix set and incrementBy calls.')
    }
    super.set(val)
  }

  /**
   * Updates the field's value by an offset. Doesn't perform optimisitic
   * locking on write. May not mix usages of set and incrementBy.
   * @param {Number} diff The diff amount.
   */
  incrementBy (diff) {
    if (this.__diff === undefined) {
      if (this.mutated &&
          this.__written) {
        // If value is mutated, but field hasn't been written, the change must
        // have been from the default value or DB. Don't throw in that case.
        throw new Error('May not mix set and incrementBy calls.')
      }
      this.__diff = 0
    }
    this.__diff += diff
    const initialVal = this.__initialValue || 0

    // Call directly on super to avoid exception
    super.set(initialVal + this.__diff)
  }

  get shouldLock () {
    return this.__diff === undefined || this.__initialValue === undefined
  }

  __updateExpression (exprKey) {
    if (!this.shouldLock) {
      return [
        `${this.name}=${this.name}+${exprKey}`,
        { [exprKey]: this.__diff },
        false
      ]
    }
    return super.__updateExpression(exprKey)
  }

  __conditionExpression (exprKey) {
    if (!this.shouldLock) {
      return []
    }
    return super.__conditionExpression(exprKey)
  }
}

/**
 * @extends Internal.__Field
 * @memberof Internal.Fields
 * @private
 */
class StringField extends __Field {}

/**
 * @extends Internal.__Field
 * @memberof Internal.Fields
 * @private
 */
class ObjectField extends __Field {
  /**
   * This method checks for equality deeply against the initial
   * value so use it as sparsely as possible. It is primarily meant to be
   * used internally for deciding whether a field needs to be transmitted to
   * the server.
   *
   * @returns if value was changed.
   */
  get mutated () {
    return !deepeq(this.__value, this.__initialValue)
  }
}

/**
 * @extends Internal.__Field
 * @memberof Internal.Fields
 * @private
 */
class BooleanField extends __Field {}

/**
 * @extends Internal.__Field
 * @memberof Internal.Fields
 * @private
 */
class ArrayField extends __Field {
  /**
   * This method checks for equality deeply against the initial
   * value so use it as sparsely as possible. It is primarily meant to be
   * used internally for deciding whether a field needs to be transmitted to
   * the server.
   *
   * @returns if value was changed.
   */
  get mutated () {
    return !deepeq(this.__value, this.__initialValue)
  }
}

const schemaTypeToFieldClassMap = {
  array: ArrayField,
  boolean: BooleanField,
  float: NumberField,
  integer: NumberField,
  number: NumberField,
  object: ObjectField,
  string: StringField
}
const schemaTypeToJSTypeMap = {
  array: Array,
  boolean: Boolean,
  integer: Number,
  number: Number,
  float: Number,
  object: Object,
  string: String
}

/**
 * Key object to identify models.
 */
class Key {
  /**
   * @param {Model} Cls a Model class
   * @param {Object} compositeID maps partition and sort key component names to
   *   their values
   * @private
   */
  constructor (Cls, compositeID) {
    this.Cls = Cls
    this.compositeID = compositeID
  }
}

/**
 * The base class for modeling data.
 * @public
 *
 * @property {Boolean} isNew Whether the item exists on server.
 */
class Model {
  /**
   * Constructs a model. Model has one `id` field. Subclasses should add
   * additional fields by overriding the constructor.
   */
  constructor () {
    this.isNew = false
    this.__written = false
    // After __setupModel() is called, __db_attrs will contain a __Field
    // subclass object which for each attribute to be written to the database.
    // There is one entry for each entry in FIELDS, plus an _id field (the
    // partition key) and optionally an _sk field (the optional sort key).
    this.__db_attrs = {}

    for (const [name, opts] of Object.entries(this.constructor.__VIS_ATTRS)) {
      const Cls = schemaTypeToFieldClassMap[opts.schema.type]
      this[name] = new Cls(opts)
    }
    // add the implicit, computed "_id" field
    this._id = new StringField(__Field.__validateFieldOptions(
      'HASH', '_id', S.string().minLength(1)))
    if (this.constructor.__hasSortKey()) {
      // add the implicit, computed "_sk" field
      this._sk = new StringField(__Field.__validateFieldOptions(
        'RANGE', '_sk', S.string().minLength(1)))
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

    // put key into the standard, non-shorthand format
    const keyComponents = {}
    function defineKey (kind, opts) {
      if (!opts) {
        if (kind === 'partition') {
          throw new InvalidFieldError('KEY', 'the partition key is required')
        } else {
          keyComponents[kind] = {}
        }
      } else if (opts.isFluentSchema || opts.schema) {
        throw new InvalidFieldError('key', 'must define key component name(s)')
      } else {
        keyComponents[kind] = opts
      }
    }
    defineKey('partition', this.KEY)
    defineKey('sort', this.SORT_KEY)

    // determine the order for the component(s) of each key type
    this.__KEY_ORDER = {}
    for (const [keyType, values] of Object.entries(keyComponents)) {
      const keyComponentNames = Object.keys(values)
      keyComponentNames.sort()
      this.__KEY_ORDER[keyType] = keyComponentNames
    }
    if (!this.__KEY_ORDER.partition.length) {
      throw new InvalidFieldError(
        'KEY', 'must define at least one partition key field')
    }
    this.__KEY_COMPONENT_NAMES = new Set()

    // cannot use the names of non-static Model members (only need to list
    // those that are defined by the constructor; those which are on the
    // prototype are enforced automatically)
    const reservedNames = new Set(['isNew'])
    const fieldsByKeyType = {
      HASH: keyComponents.partition,
      RANGE: keyComponents.sort,
      '': this.FIELDS
    }
    const proto = this.prototype

    // __VIS_ATTRS maps the name of attributes that are visible to users of
    // this model. This is the combination of attributes (keys) defined by KEY,
    // SORT_KEY and FIELDS.
    this.__VIS_ATTRS = {}
    for (const [keyType, props] of Object.entries(fieldsByKeyType)) {
      for (const [fieldName, schema] of Object.entries(props)) {
        if (this.__VIS_ATTRS[fieldName]) {
          throw new InvalidFieldError(
            fieldName, 'property name cannot be used more than once')
        }
        const finalFieldOpts = __Field.__validateFieldOptions(
          keyType || undefined, fieldName, schema)
        this.__VIS_ATTRS[fieldName] = finalFieldOpts
        if (keyType) {
          this.__KEY_COMPONENT_NAMES.add(fieldName)
        }
        if (reservedNames.has(fieldName)) {
          throw new InvalidFieldError(
            fieldName, 'this name is reserved and may not be used')
        }
        if (fieldName in proto) {
          throw new InvalidFieldError(fieldName, 'shadows another name')
        }
      }
    }
  }

  static __getResourceDefinition () {
    this.__doOneTimeModelPrep()
    // the partition key attribute is always "_id" and of type string
    const attrs = [{ AttributeName: '_id', AttributeType: 'S' }]
    const keys = [{ AttributeName: '_id', KeyType: 'HASH' }]

    // if we have a sort key attribute, it always "_sk" and of type string
    if (this.__KEY_ORDER.sort.length) {
      attrs.push({ AttributeName: '_sk', AttributeType: 'S' })
      keys.push({ AttributeName: '_sk', KeyType: 'RANGE' })
    }
    return {
      TableName: this.fullTableName,
      AttributeDefinitions: attrs,
      KeySchema: keys
    }
  }

  /**
   * Defines the partition key. Every item in the database is uniquely
   * identified by the combination of its partition and sort key. The default
   * partition key is a UUIDv4.
   *
   * A key can simply be some scalar value:
   *   KEY = { id: S.string() }
   *
   * A key may can be "compound key", i.e., a key with one or components, each
   * with their own name and schema:
   *   KEY = {
   *     email: S.string().format(S.FORMATS.EMAIL),
   *     birthYear: S.integer().minimum(1900)
   *   }
   */
  static KEY = { id: S.string().format(S.FORMATS.UUID) }

  /** Defines the sort key, if any. Uses the compound key format from KEY. */
  static SORT_KEY = {}

  /**
   * Defines the non-key fields. By default there are no fields.
   *
   * Properties are defined as a map from field names to a fluent-schema:
   * @example
   *   FIELDS = {
   *     someNumber: S.number(),
   *     someNumberWithOptions: S.number().optional().default(0).readOnly()
   *   }
   */
  static FIELDS = {}

  /**
   * Returns true if this object has a sort key.
   */
  static __hasSortKey () {
    return this.__KEY_ORDER.sort.length > 0
  }

  /**
   * Returns a map containing the model's computed key values (_id, as well as
   * _sk if model has a sort key).
   * Verifies that the keys are valid (i.e., they match the required schema).
   * @param {Object} vals map of field names to values
   * @returns map of _id and (if hasSortKey()) _sk attribute values
   */
  static __computeKeyAttrMap (vals) {
    // compute and validate the partition attribute
    const keyAttrs = {
      _id: this.__encodeCompoundValueToString(this.__KEY_ORDER.partition, vals)
    }

    // add and validate the sort attribute, if any
    if (this.__hasSortKey()) {
      keyAttrs._sk = this.__encodeCompoundValueToString(
        this.__KEY_ORDER.sort, vals
      )
    }
    return keyAttrs
  }

  /**
   * Returns the underlying __Field associated with an attribute.
   *
   * @param {String} name the name of a field from FIELDS
   * @returns {BooleanField|ArrayField|ObjectField|NumberField|StringField}
   */
  getField (name) {
    assert.ok(!name.startsWith('_'), 'may not access internal computed fields')
    return this.__db_attrs[name]
  }

  /**
   * The table name this model is associated with, excluding the service ID
   * prefix. This is the model's class name. However, suclasses may choose to
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
   * 2-tuple, [compositeID, modelData].
   *
   * @param {Object} data data to be split
   */
  __splitIDFromOtherFields (data) {
    const compositeID = {}
    const modelData = {}
    Object.keys(data).forEach(key => {
      const value = this.__db_attrs[key] || this[key]
      if (value instanceof __Field &&
          value.keyType !== undefined) {
        compositeID[key] = data[key]
      } else {
        modelData[key] = data[key]
      }
    })
    this.constructor.__computeKeyAttrMap(compositeID)
    return [compositeID, modelData]
  }

  /**
   * @access package
   * @param {CompositeID} compositeID
   * @param {GetParams} [options]
   * @returns {Object} parameters for a get request to DynamoDB
   */
  __getParams (compositeID, options) {
    return {
      TableName: this.__fullTableName,
      ConsistentRead: options && !options.inconsistentRead,
      Key: compositeID
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
    if (this.__initMethod === Model.__INIT_METHOD.UPDATE) {
      // This is really unreachable code.
      // The only way to get here is when the model is mutated (to complete a
      // write) and has no field mutated (so PUT is used instead of UPDATE).
      // It can happen only when the model isNew.
      // However, when items are setup from updateItem method, we pretend the
      // items to be not new. Hence, the condition will never be satisfied.
      // conditions.push('attribute_exists(_id)')
      assert.fail('This should be unreachable unless something is broken.')
    }

    const item = {}
    const accessedFields = []
    let exprCount = 0
    Object.keys(this.__db_attrs).forEach(key => {
      const field = this.__db_attrs[key]
      field.validate()

      if (field.__value !== undefined) {
        // Not having undefined keys effectively removes them.
        // Also saves some bandwidth.
        item[key] = deepcopy(field.__value)
      }

      if (field.keyType === undefined) {
        // Put works by overriding the entire item,
        // all fields needs to be written.
        // No need to check for field.accessed, pretend everything is accessed,
        // except for keys, since they don't change
        accessedFields.push(field)
      }
    })

    let conditionExpr
    let hasExistsCheck = false
    const isCreateOrPut =
      this.__initMethod === Model.__INIT_METHOD.CREATE_OR_PUT
    const exprValues = {}
    if (this.isNew) {
      if (isCreateOrPut) {
        const conditions = []
        for (const field of accessedFields) {
          const exprKey = `:_${exprCount++}`
          const [condition, vals] = field.__conditionExpression(exprKey)
          if (condition &&
            (!isCreateOrPut || !condition.startsWith('attribute_not_exists'))) {
            conditions.push(condition)
            Object.assign(exprValues, vals)
          }
        }
        conditionExpr = conditions.join(' AND ')

        if (conditionExpr.length !== 0) {
          conditionExpr = `attribute_not_exists(#id) OR (${conditionExpr})`
          hasExistsCheck = true
        }
      } else {
        conditionExpr = 'attribute_not_exists(#id)'
        hasExistsCheck = true
      }
    } else {
      const conditions = []
      for (const field of accessedFields) {
        const exprKey = `:_${exprCount++}`
        const [condition, vals] = field.__conditionExpression(exprKey)
        if (condition) {
          conditions.push(condition)
          Object.assign(exprValues, vals)
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
    if (hasExistsCheck) {
      ret.ExpressionAttributeNames = { '#id': '_id' }
    }
    return ret
  }

  /**
   * Generates parameters for an update request to DynamoDB.
   * Update only overrides fields that got updated to a different value.
   * Untracked fields will not be removed from DynamoDB. This library supports
   * optimistic locking for update. Since update only touches specific fields
   * of an item, optimisitc locking is only performed on fields accessed (read
   * or write). This locking mechanism results in less likely contentions,
   * hence is prefered over put.
   *
   * @access package
   * @param {Boolean} shouldValidate Whether each field needs to be validated.
   *   If undefined, default behavior is to have validation.
   *   It is used for generating params for ConditionCheck which is mostly
   *   identical to updateParams. But omit validation since the model is either
   *   from server which must be valid already (from validations on last
   *   write), or fields sitll need to be setup before they are all valid.
   * @returns parameters for a update request to DynamoDB
   */
  __updateParams (shouldValidate) {
    const conditions = []
    const exprValues = {}
    const itemKey = {}
    const sets = []
    const removes = []
    const accessedFields = []
    let exprCount = 0

    const isUpdate =
      this.__initMethod === Model.__INIT_METHOD.UPDATE

    Object.keys(this.__db_attrs).forEach(key => {
      const field = this.__db_attrs[key]
      const omitInUpdate = isUpdate && field.get() === undefined
      const doValidate = (shouldValidate === undefined || shouldValidate) &&
        !omitInUpdate
      if (doValidate) {
        field.validate()
      }

      if (field.keyType !== undefined) {
        itemKey[field.name] = field.__value
        return
      }

      if (omitInUpdate) {
        // When init method is UPDATE, not all required fields are present in the
        // model: we only write parts of the model.
        // Hence we exclude any fields that are not part of the update.
        return
      }

      const exprKey = `:_${exprCount++}`
      const [set, vals, remove] = field.__updateExpression(exprKey)
      if (set) {
        sets.push(set)
        Object.assign(exprValues, vals)
      }
      if (remove) {
        removes.push(field.name)
      }

      if (field.accessed) {
        accessedFields.push(field)
      }
    })

    let hasExistsCheck = false
    if (this.isNew) {
      conditions.push('attribute_not_exists(#id)')
      hasExistsCheck = true
    } else {
      if (isUpdate) {
        conditions.push('attribute_exists(#id)')
        hasExistsCheck = true
      }

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
      // NOTE: This is optional in dynamodb's update call,
      // but required in the transactWrite.update conterpart.
      ret.UpdateExpression = actions.join(' ')
    }
    if (conditions.length) {
      ret.ConditionExpression = conditions.join(' AND ')
    }
    if (Object.keys(exprValues).length) {
      ret.ExpressionAttributeValues = exprValues
    }
    if (hasExistsCheck) {
      ret.ExpressionAttributeNames = { '#id': '_id' }
    }
    return ret
  }

  /**
   * Indicates if any field was mutated. New models are considered to be
   * mutated as well.
   *
   * @type {Boolean}
   */
  __isMutated () {
    return this.isNew || Object.values(this.__db_attrs).reduce(
      (result, field) => {
        return result || field.mutated
      },
      false)
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
    const ret = this.__updateParams(false)
    if (ret.ConditionExpression) {
      return ret
    }
    return undefined
  }

  static get __INIT_METHOD () {
    return {
      CREATE: 1,
      GET: 2,
      UPDATE: 3,
      CREATE_OR_PUT: 4
    }
  }

  __setupKey (isPartitionKey, vals) {
    let attrName, keyOrderKey
    if (isPartitionKey) {
      attrName = '_id'
      keyOrderKey = 'partition'
    } else {
      attrName = '_sk'
      keyOrderKey = 'sort'
    }
    const keyOrder = this.constructor.__KEY_ORDER[keyOrderKey]
    if (!vals[attrName]) {
      // if the computed field is missing, compute it
      vals[attrName] = this.constructor.__encodeCompoundValueToString(
        keyOrder, vals)
    } else {
      // if the components of the computed field are missing, compute them
      /* istanbul ignore else */
      if (vals[keyOrder[0]] === undefined) {
        Object.assign(vals, this.constructor.__decodeCompoundValueFromString(
          keyOrder, vals[attrName], attrName))
      }
    }
  }

  /**
   * Sets up a model, restricts access to the model afterwards, e.g. can no
   * longer add properties.
   * @access package
   *
   * @param {Object} vals values to use for populating fields.
   * @param {Boolean} isNew whether the data exists on server.
   * @param {Model.__INIT_METHOD} method How the model was instantiated.
   */
  __setupModel (vals, isNew, method) {
    this.__setupKey(true, vals)
    if (this.constructor.__hasSortKey()) {
      this.__setupKey(false, vals)
    }

    this.isNew = !!isNew
    const methods = Model.__INIT_METHOD
    if (!Object.values(methods).includes(method)) {
      throw new InvalidParameterError('method',
        'must be one of CREATE or GET.')
    }
    this.__initMethod = method
    Object.keys(this).forEach(key => {
      const field = this[key]
      if (field instanceof __Field) {
        field.name = key

        const keyType = field.keyType
        if (!keyType || key === '_id' || key === '_sk') {
          // key fields are implicitly included in the "_id" or "_sk" field;
          // they are otherwise ignored!
          this.__db_attrs[key] = field
        }
        const val = vals[key]
        field.__setup(val)

        if (keyType !== undefined) {
          // At this point, new models might not have all the necessary setups,
          // but all key fields should be valid.
          field.validate()
        }

        Object.defineProperty(this, key, {
          get: (...args) => {
            return field.get()
          },
          set: (val) => {
            field.set(val)
          }
        })
      }
    })

    // Once setup, restrict access to model.
    Object.seal(this)
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
  static __encodeCompoundValueToString (keyOrder, values) {
    const pieces = []
    for (var i = 0; i < keyOrder.length; i++) {
      const fieldName = keyOrder[i]
      const fieldOpts = this.__VIS_ATTRS[fieldName]
      const givenValue = values[fieldName]
      if (givenValue === undefined) {
        throw new InvalidFieldError(fieldName, 'must be provided')
      }
      const valueType = validateValue(fieldName, fieldOpts, givenValue)
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
        pieces.push(JSON.stringify(givenValue))
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
  static __decodeCompoundValueFromString (keyOrder, strVal, attrName) {
    const compoundID = {}
    const pieces = strVal.split('\0')
    if (pieces.length !== keyOrder.length) {
      throw new InvalidFieldError(
        attrName, 'failed to parse key: incorrect number of components')
    }
    for (let i = 0; i < pieces.length; i++) {
      const piece = pieces[i]
      const fieldName = keyOrder[i]
      const fieldOpts = this.__VIS_ATTRS[fieldName]
      const valueType = schemaTypeToJSTypeMap[fieldOpts.schema.type]
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
   * Create a Key for a unique row in the DB table associated with this model.
   * @param {*} keyValues map of key component names to values; if there is
   *   only one partition key field (whose type is not object), then this MAY
   *   instead be just that field's value
   * @returns {Key} a Key object.
   */
  static key (keyValues) {
    if (!keyValues) {
      throw new InvalidParameterError('keyValues', 'missing')
    }

    // if we only have one key component, then the `_id` **MAY** just be the
    // value rather than a map of key component names to values
    assert.ok(this.__KEY_ORDER,
      `model ${this.name} one-time setup was not done (remember to export ` +
      'the model and in unit tests remember to call createUnittestResource()')
    const pKeyOrder = this.__KEY_ORDER.partition
    if (pKeyOrder.length === 1 && !this.__KEY_ORDER.sort.length) {
      const pFieldName = pKeyOrder[0]
      if (!(keyValues instanceof Object) || !keyValues[pFieldName]) {
        keyValues = { [pFieldName]: keyValues }
      }
    }
    if (!(keyValues instanceof Object)) {
      throw new InvalidParameterError('keyValues',
        'should be an object mapping key component names to values')
    }

    // check if we were given too many keys (if too few, then the validation
    // will fail, so we don't need to handle that case here)
    const givenKeys = Object.keys(keyValues)
    const numKeysGiven = givenKeys.length
    if (numKeysGiven > this.__KEY_COMPONENT_NAMES.size) {
      const excessKeys = []
      for (const givenKey of givenKeys) {
        if (!this.__KEY_COMPONENT_NAMES.has(givenKey)) {
          excessKeys.push(givenKeys)
        }
      }
      excessKeys.sort()
      const expKeys = [...this.__KEY_COMPONENT_NAMES]
      expKeys.sort()
      throw new InvalidParameterError('keyValues',
        `expected keys ${expKeys.join(', ')} but got ${givenKeys.join(', ')}`)
    }
    return new Key(this, this.__computeKeyAttrMap(keyValues))
  }

  /**
   * Writes model to database. Uses DynamoDB update under the hood.
   * @access package
   */
  async __write () {
    assert.ok(!this.__written, 'May write once')
    this.__written = true

    const usePut =
      this.__initMethod === Model.__INIT_METHOD.CREATE_OR_PUT
    const method = usePut ? 'put' : 'update'
    const params = this[`__${method}Params`]()
    const retries = 3
    let millisBackOff = 40
    for (let tryCnt = 0; tryCnt <= retries; tryCnt++) {
      try {
        await this.documentClient[method](params).promise()
        return
      } catch (error) {
        if (!error.retryable) {
          if (this.__initMethod === Model.__INIT_METHOD.CREATE &&
              error.code === 'ConditionalCheckFailedException') {
            throw new ModelAlreadyExistsError(this._id, this._sk)
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
   * Shows the type and key of a model, for example,
   * [Model Foo:paritionKey:sortKey], so that each model has a unique
   * identifier to be used in Object and Set.
   */
  toString () {
    let keyStr = this.__db_attrs._id.__value
    if (this.constructor.__hasSortKey()) {
      keyStr += this.__db_attrs._sk.__value
    }
    return `[Model ${this.constructor.name}:${keyStr}]`
  }
}

async function getWithArgs (args, callback) {
  if (!args || !(args instanceof Array) || args.length === 0) {
    throw new InvalidParameterError('args', 'must be a non-empty array')
  }
  const [first, ...args1] = args
  if (first && first.prototype instanceof Model) {
    if (args1.length === 1 || args1.length === 2) {
      const key = first.key(args1[0])
      return getWithArgs([key, ...args1.slice(1)], callback)
    } else {
      throw new InvalidParameterError('args',
        'Expecting args to have a tuple of (Model, keyValues, optionalOpt).')
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
    return Promise.all(first.map(key => callback(key, params)))
  } else {
    throw new InvalidParameterError('args',
      'Expecting Model or Key or [Key] as the first argument')
  }
}

/**
 * Batches put and update (potentially could support delete) requests to
 * DynamoDB within a transaction and sents on commit.
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
   * Favors update over put for writting to DynamoDB, except for a corner case
   * where update disallows write operations without an UpdateExpression. This
   * happens when a new model is created with no fields besides keys populated
   * and written to DB.
   *
   * @param {Model} model the model to write
   * @access private
   */
  async __write (model) {
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
    this.__toCheck[model] = false

    let action = 'Update'
    let params = model.__updateParams()
    if (!Object.prototype.hasOwnProperty.call(
      params,
      'UpdateExpression'
    )) {
      action = 'Put'
      params = model.__putParams()
    }
    if (model.__initMethod === model.constructor.__INIT_METHOD.CREATE) {
      params.ReturnValuesOnConditionCheckFailure = 'ALL_OLD'
    }
    this.__toWrite.push({ [action]: params })
  }

  /**
   * Start tracking models in a transaction. So when the batched write commits,
   * Optimistic locking on those readonly models is automatically performed.
   * @param {Model} model A model to track.
   */
  track (model) {
    assert.ok(this.__toCheck[model] === undefined,
      `Model ${model.toString()} already tracked`)
    this.__allModels.push(model)
    this.__toCheck[model] = model
  }

  /**
   * Commits batched writes by sending DynamodDB requests.
   *
   * @returns {Boolean} whether any model is written to DB.
   */
  async commit () {
    assert.ok(!this.resolved, 'Already wrote models.')
    this.resolved = true

    for (const model of this.__allModels) {
      if (this.__toCheck[model] && model.__isMutated()) {
        await this.__write(model)
      }
    }

    if (!this.__toWrite.length) {
      return false
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
    const request = this.documentClient.transactWrite(params)
    /* istanbul ignore next */
    request.on('extractError', (response) => {
      this.__extractError(response)
    })
    await request.promise()
    return true
  }

  __extractError (response) {
    const responseBody = response.httpResponse.body.toString()
    const reasons = JSON.parse(responseBody).CancellationReasons
    for (const reason of reasons) {
      if (reason.Code === 'ConditionalCheckFailed' &&
          reason.Item &&
          Object.keys(reason.Item).length) {
        // We only ask for the object to be returned when it's `created`.
        // If we see ConditionalCheckFailed and an Item we know it's due
        // to creating an existing item.
        const itemId = Object.values(reason.Item._id)[0]
        const _sk = reason.Item._sk
        const itemSk = (_sk !== undefined) ? Object.values(_sk)[0] : undefined
        const error = new ModelAlreadyExistsError(itemId, itemSk)
        error.name = reason.Code
        error.retryable = false
        throw error
      }
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
   * @property {Number} [retries=3] The number of times to retry after the
   *   initial attempt fails.
   * @property {Number} [initialBackoff=500] In milliseconds, delay
   *   after the first attempt fails and before first retry happens.
   * @property {Number} [maxBackoff=10000] In milliseconds, max delay
   *   between retries. Must be larger than 200.
   */

  /**
   * Returns the default [options]{@link TransactionOptions} for a transaction.
   */
  get defaultOptions () {
    return {
      retries: 3,
      initialBackoff: 500,
      maxBackoff: 10000
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
   * Fetches model(s) from database.
   * This method supports 3 different signatures.
   *   get(Cls, keyValues, params)
   *   get(Key, params)
   *   get([Key], params)
   *
   * @param {Model} Cls a Model class.
   * @param {String|CompositeID} key Key or keyValues
   * @param {GetParams} [params]
   * @returns Model(s) associated with provided key
   */
  async get (...args) {
    return getWithArgs(args, async (key, params) => {
      const model = new key.Cls(params)
      const getParams = model.__getParams(key.compositeID, params)
      const data = await this.documentClient.get(getParams).promise()
      if ((!params || !params.createIfMissing) && !data.Item) {
        return undefined
      }
      model.__setupModel(data.Item || { ...key.compositeID }, !data.Item,
        model.constructor.__INIT_METHOD.GET)
      this.__writeBatcher.track(model)
      return model
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
   * @param {Object} [params] Parameters to be passed to model's constructor
   */
  update (Cls, original, updated, params) {
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

    const model = new Cls(params)
    const data = model.__splitIDFromOtherFields(original)[1] // also check keys
    model.__setupModel(original, false, model.constructor.__INIT_METHOD.UPDATE)
    Object.keys(data).forEach(k => {
      model.getField(k).get() // Read to show in ConditionExpression
    })

    Object.keys(updated).forEach(key => {
      if (model.constructor.__VIS_ATTRS[key].keyType !== undefined) {
        throw new InvalidParameterError(
          'updated', 'must not contain key fields')
      }
      model[key] = updated[key]
    })

    this.__writeBatcher.track(model)

    // Don't return model, since it should be closed to futhur modifications.
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
    const model = new Cls()
    model.__splitIDFromOtherFields(original)
    model.__setupModel(original, true,
      model.constructor.__INIT_METHOD.CREATE_OR_PUT)
    Object.keys(updated).forEach(key => {
      model[key] = updated[key]
    })
    const fieldNames = Object.keys(model.constructor.__VIS_ATTRS)
    const missingFields = fieldNames.filter(key => {
      return !Object.prototype.hasOwnProperty.call(original, key) &&
        !Object.prototype.hasOwnProperty.call(updated, key)
    })
    if (missingFields.length) {
      throw new InvalidParameterError(
        'updated',
        `is missing keys ${missingFields}`)
    }
    this.__writeBatcher.track(model)

    // Don't return model, since it should be closed to futhur modifications.
    // return model
  }

  /**
   * Creates a model without accesing DB. Write will make sure the item does
   * not exist.
   *
   * @param {Model} Cls A Model class.
   * @param {CompositeID|Object} data A superset of CompositeID of the model,
   *   plus any data for Fields on the Model.
   */
  create (Cls, data) {
    const model = new Cls()
    const [compositeID, modelData] = model.__splitIDFromOtherFields(data)
    model.__setupModel(compositeID, true,
      model.constructor.__INIT_METHOD.CREATE)
    for (const [key, val] of Object.entries(modelData)) {
      model[key] = val
    }
    this.__writeBatcher.track(model)
    return model
  }

  __reset () {
    this.__writeBatcher = new __WriteBatcher()
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

  /**
   * Runs a closure in transaction.
   * @param {Function} func the closure to run
   * @access private
   */
  async __run (func) {
    if (!(func instanceof Function)) {
      throw new InvalidParameterError('func', 'must be a function / closure')
    }

    let millisBackOff = this.options.initialBackoff
    const maxBackoff = this.options.maxBackoff
    for (let tryCnt = 0; tryCnt <= this.options.retries; tryCnt++) {
      try {
        this.__reset()
        const ret = await func(this)
        await this.__writeBatcher.commit()
        return ret
      } catch (err) {
        if (!this.constructor.__isRetryable(err)) {
          throw err
        } else {
          console.log(`Transaction commit attempt ${tryCnt} failed with ` +
            `error ${err}.`)
        }
      }
      if (tryCnt >= this.options.retries) {
        throw new TransactionFailedError('Too much contention.')
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
    switch (args.length) {
      case 1:
        return new Transaction({}).__run(args[0])
      case 2:
        return new Transaction(args[0]).__run(args[1])
      default:
        throw new InvalidParameterError('args',
          'should be (options, func) or (func)')
    }
  }
}

function makeCreateUnittestResourceFunc (dynamoDB) {
  return async function () {
    this.__doOneTimeModelPrep()
    const params = this.__getResourceDefinition()
    params.ProvisionedThroughput = {
      ReadCapacityUnits: 2,
      WriteCapacityUnits: 2
    }
    await dynamoDB.createTable(params).promise().catch(err => {
      /* istanbul ignore if */
      if (err.code !== 'ResourceInUseException') {
        throw err
      }
    })
  }
}

/* istanbul ignore next */
const DefaultConfig = {
  awsConfig: {
    region: 'us-west-2',
    endpoint: process.env.DYNAMO_ENDPT || ''
  },
  enableDAX: true
}

/**
 * @module dynamodb
 */

/**
 * Setup the DynamoDB library before returning symbols clients can use.
 *
 * @param {Object} [config] Configurations for the library
 * @param {Object} [config.awsConfig] Config supported by AWS client.
 * @param {Boolean} [config.enableDAX=true] Whether to use DAX or plain
 *   DynamoDB.
 * @returns {Object} Symbols that clients of this library can use.
 * @private
 */
function setup (config) {
  config = loadOptionDefaults(config, DefaultConfig)
  const awsConfig = loadOptionDefaults(config.awsConfig,
    DefaultConfig.awsConfig)

  const AWS = require('aws-sdk')
  const dynamoDB = new AWS.DynamoDB(awsConfig)
  let documentClient

  const inDebugger = !!Number(process.env.INDEBUGGER)
  /* istanbul ignore if */
  if (config.enableDAX &&
      !inDebugger &&
      process.env.DAX_ENDPOINT) {
    const AwsDaxClient = require('amazon-dax-client')
    awsConfig.endpoints = [process.env.DAX_ENDPOINT]
    const daxDB = new AwsDaxClient(awsConfig)
    documentClient = new AWS.DynamoDB.DocumentClient({ service: daxDB })
  } else {
    documentClient = new AWS.DynamoDB.DocumentClient({ service: dynamoDB })
  }

  // Make DynamoDB clients available to these classes
  const clsWithDBAccess = [
    Model,
    Transaction,
    __WriteBatcher
  ]
  clsWithDBAccess.forEach(Cls => {
    Cls.documentClient = documentClient
    Cls.prototype.documentClient = documentClient
  })

  if (inDebugger) {
    // For creating tables in debug environments
    Model.createUnittestResource = makeCreateUnittestResourceFunc(dynamoDB)
  }

  const exportAsClass = {
    Model,
    Transaction,

    // Errors
    InvalidOptionsError,
    InvalidParameterError,
    InvalidFieldError,
    TransactionFailedError,
    ModelAlreadyExistsError
  }

  const toExport = Object.assign({}, exportAsClass)
  if (inDebugger) {
    toExport.__private = {
      __Field,
      __WriteBatcher,
      getWithArgs
    }
    const exportAsFactory = [
      ArrayField,
      BooleanField,
      NumberField,
      ObjectField,
      StringField
    ]
    exportAsFactory.forEach(Cls => {
      toExport.__private[Cls.name] = options => {
        options = options || {}
        let schema
        function processOption (key, func) {
          if (Object.hasOwnProperty.call(options, key)) {
            const val = options[key]
            schema = func(val)
            delete options[key]
            return val
          }
        }
        // schema is required; fill in the default if none is provided
        processOption('schema', schema => schema)
        if (!schema) {
          if (Cls === ArrayField) {
            schema = S.array()
          } else if (Cls === BooleanField) {
            schema = S.boolean()
          } else if (Cls === NumberField) {
            schema = S.number()
          } else if (Cls === ObjectField) {
            schema = S.object()
          } else {
            assert.ok(Cls === StringField, 'unexpected class: ' + Cls.name)
            schema = S.string()
          }
        }
        const keyType = processOption('keyType', () => schema)
        processOption('optional', isOpt => isOpt ? schema.optional() : schema)
        processOption('immutable', isReadOnly => schema.readOnly(isReadOnly))
        processOption('default', val => schema.default(val))
        const optionKeysLeft = Object.keys(options)
        assert.ok(optionKeysLeft.length === 0,
          `unexpected option(s): ${optionKeysLeft}`)
        options = __Field.__validateFieldOptions(keyType, 'someName', schema)
        return new Cls(options)
      }
    })
  }
  return toExport
}
module.exports = setup()
