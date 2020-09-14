const assert = require('assert')
const EventEmitter = require('events')

const ajv = new (require('ajv'))({
  allErrors: true,
  removeAdditional: 'failing'
})
const deepeq = require('deep-equal')
const S = require('fluent-schema')
const deepcopy = require('rfdc')()

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
    super(`${field} ${reason}`)
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
    this.retryable = false
  }
}

/**
 * Thrown when a model is to be updated, but condition check failed.
 */
class InvalidModelUpdateError extends Error {
  constructor (_id, _sk) {
    const skStr = (_sk !== undefined) ? ` _sk=${_sk}` : ''
    super(`Tried to update model _id=${_id}${skStr} ` +
      'with outdated / invalid conditions')
    this.name = this.constructor.name
    this.retryable = false
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
      optional: schema.optional === true,
      immutable: isKey || schema.readOnly === true,
      default: schema.default
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
      if (hasDefault && keyType === 'HASH') {
        throw new InvalidOptionsError('default',
          'No defaults for partition keys. It just doesn\'t make sense.')
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
   * @param {boolean} mayUseDefault only used if isForNewItem is true
   * @param {boolean} valIsFromDB whether the value is from the database (or is
   *   expected to be the value in the database)
   */
  constructor (name, options, val, valIsFromDB, mayUseDefault, isPartial,
    noSeal) {
    for (const [key, value] of Object.entries(options)) {
      Object.defineProperty(this, key, { value, writable: false })
    }

    // Setup states
    /**
     * @memberof Internal.__Field
     * @instance
     * @member {String} name The name of the owning property.
     */
    this.name = name // Will be set after params for model are setup
    this.__initialValue = undefined
    this.__value = undefined
    this.__read = false // If get is called
    this.__written = false // If set is called
    this.__default = options.default // only used for new items!
    if (!noSeal) { // subclasses may need to add properties
      Object.seal(this)
    }

    if (!valIsFromDB) {
      // use the default if no value is provided (if undefined is
      // explicitly provided as a value, then it is used as the value
      // [that is only valid for optional fields, as an undefined value
      // means the field will not be stored on the server])
      mayUseDefault = mayUseDefault && this.__default !== undefined
      val = mayUseDefault ? deepcopy(this.__default) : val
      this.__value = val
      this.validate()
    } else {
      // make a copy of the value we got from the database
      this.__initialValue = deepcopy(val)
      this.__value = val
      // keys already validated by Model's __setupKey()
      // if the object is partial, don't validate missing values
      if (!options.keyType && (!isPartial || val !== undefined)) {
        this.validate()
      }
    }
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
    if (this.immutable) {
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
  constructor (name, options, val, isForNewItem, mayUseDefault, isPartial) {
    super(name, options, val, isForNewItem, mayUseDefault, isPartial, true)
    this.__diff = undefined
    Object.seal(this)
  }

  set (val) {
    if (this.__diff !== undefined) {
      throw new Error('May not mix set and incrementBy calls.')
    }
    super.set(val)
  }

  /**
   * Updates the field's value by an offset. Doesn't perform optimistic
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
   * @param {Object} encodedKeys map of encoded partition and sort key
   * @param {Object} keyComponents key component values
   * @param {Object} [fields] field (non-key) values
   * @private
   */
  constructor (Cls, encodedKeys, keyComponents, fields) {
    this.Cls = Cls
    this.encodedKeys = encodedKeys
    this.keyComponents = keyComponents
    this.data = fields
  }

  get vals () {
    return { ...this.keyComponents, ...this.data }
  }
}

const _ID_FIELD_OPTS = __Field.__validateFieldOptions(
  'HASH', '_id', S.string().minLength(1))
const _SK_FIELD_OPTS = __Field.__validateFieldOptions(
  'RANGE', '_sk', S.string().minLength(1))

// sentinel values for different item creation methods
const ITEM_SOURCE = {
  CREATE: { isCreate: true },
  GET: { isGet: true },
  UPDATE: { isUpdate: true },
  CREATE_OR_PUT: { isCreateOrPut: true }
}
const ITEM_SOURCES = new Set(Object.values(ITEM_SOURCE))

/**
 * The base class for modeling data.
 * @public
 *
 * @property {Boolean} isNew Whether the item exists on server.
 */
class Model {
  /**
   * Create a representation of a database Item. Should only be used by the
   * library.
   * @private
   */
  constructor (src, isNew, vals) {
    this.isNew = !!isNew

    if (!ITEM_SOURCES.has(src)) {
      throw new InvalidParameterError('src', 'invalid item source type')
    }
    this.__src = src

    // track whether this item has been written to the db yet
    this.__written = false

    // __db_attrs has a __Field subclass object for each attribute to be
    // written to the database. There is one attribute for each entry in
    // FIELDS, plus an _id field (the partition key) and optionally an _sk
    // field (the optional sort key).
    this.__db_attrs = {}
    // __non_db_attrs has a __Field subclass object for each key component
    this.__nondb_attrs = {}

    // make sure val is populated with both the encoded keys (_id and _sk) as
    // well as each key components (i.e., the keys in KEY and SORT_KEYS)
    this.constructor.__setupKey(true, vals)
    if (this.constructor.__hasSortKey()) {
      this.constructor.__setupKey(false, vals)
    }

    // add user-defined fields from FIELDS & key components from KEY & SORT_KEY
    for (const [name, opts] of Object.entries(this.constructor.__VIS_ATTRS)) {
      this.__addField(name, opts, vals)
    }
    this.__addField('_id', _ID_FIELD_OPTS, vals)
    if (this.constructor.__hasSortKey()) {
      this.__addField('_sk', _SK_FIELD_OPTS, vals)
    }
    Object.seal(this)
  }

  __addField (name, opts, vals) {
    const val = vals[name]
    const mayUseDefault = !Object.hasOwnProperty.call(vals, name)
    const Cls = schemaTypeToFieldClassMap[opts.schema.type]
    const isPartial = this.__src.isUpdate
    const field = new Cls(
      name, opts, val, !this.isNew, mayUseDefault, isPartial)
    this[name] = field
    if (!opts.keyType || name === '_id' || name === '_sk') {
      // key fields are implicitly included in the "_id" or "_sk" field;
      // they are otherwise ignored!
      this.__db_attrs[name] = field
    } else {
      this.__nondb_attrs[name] = field
    }
    Object.defineProperty(this, name, {
      get: (...args) => {
        return field.get()
      },
      set: (val) => {
        field.set(val)
      }
    })
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
    return this.__db_attrs[name] || this.__nondb_attrs[name]
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
   * 3-tuple, [keyComponents, modelData, encodedKeys].
   *
   * @param {Object} data data to be split
   */
  static __splitKeysAndData (data) {
    const keyComponents = {}
    const modelData = {}
    Object.keys(data).forEach(key => {
      if (this.__KEY_COMPONENT_NAMES.has(key)) {
        keyComponents[key] = data[key]
      } else if (this.__VIS_ATTRS[key]) {
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
    const isCreateOrPut = this.__src.isCreateOrPut
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
   * of an item, optimistic locking is only performed on fields accessed (read
   * or write). This locking mechanism results in less likely contentions,
   * hence is preferred over put.
   *
   * @access package
   * @param {Boolean} shouldValidate Whether each field needs to be validated.
   *   If undefined, default behavior is to have validation.
   *   It is used for generating params for ConditionCheck which is mostly
   *   identical to updateParams. But omit validation since the model is either
   *   from server which must be valid already (from validations on last
   *   write), or fields still need to be setup before they are all valid.
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

    const isUpdate = this.__src.isUpdate
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

  static __setupKey (isPartitionKey, vals) {
    let attrName, keyOrderKey
    if (isPartitionKey) {
      attrName = '_id'
      keyOrderKey = 'partition'
    } else {
      attrName = '_sk'
      keyOrderKey = 'sort'
    }
    const keyOrder = this.__KEY_ORDER[keyOrderKey]
    if (!vals[attrName]) {
      // if the computed field is missing, compute it
      vals[attrName] = this.__encodeCompoundValueToString(
        keyOrder, vals)
    } else {
      // if the components of the computed field are missing, compute them
      /* istanbul ignore else */
      if (vals[keyOrder[0]] === undefined) {
        Object.assign(vals, this.__decodeCompoundValueFromString(
          keyOrder, vals[attrName], attrName))
      }
    }
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
   * @param {*} vals map of key component names to values; if there is
   *   only one partition key field (whose type is not object), then this MAY
   *   instead be just that field's value. If the key is to be used to create
   *   a new item, it must also include at least any required data fields.
   * @returns {Key} a Key object.
   */
  static key (vals) {
    if (!vals) {
      throw new InvalidParameterError('values', 'missing')
    }

    // if we only have one key component, then the `_id` **MAY** just be the
    // value rather than a map of key component names to values
    assert.ok(this.__KEY_ORDER,
      `model ${this.name} one-time setup was not done (remember to export ` +
      'the model and in unit tests remember to call createUnittestResource()')
    const pKeyOrder = this.__KEY_ORDER.partition
    if (pKeyOrder.length === 1 && !this.__KEY_ORDER.sort.length) {
      const pFieldName = pKeyOrder[0]
      if (!(vals instanceof Object) || !vals[pFieldName]) {
        vals = { [pFieldName]: vals }
      }
    }
    if (!(vals instanceof Object)) {
      throw new InvalidParameterError('values',
        'should be an object mapping key component names to values')
    }

    // check if we were given too many keys (if too few, then the validation
    // will fail, so we don't need to handle that case here)
    return new Key(this, ...this.__splitKeysAndData(vals))
  }

  /**
   * Writes model to database. Uses DynamoDB update under the hood.
   * @access package
   */
  async __write () {
    assert.ok(!this.__written, 'May write once')
    this.__written = true

    const usePut = this.__src.isCreateOrPut
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
          const isConditionalCheckFailure =
            error.code === 'ConditionalCheckFailedException'
          if (isConditionalCheckFailure && this.__src.isCreate) {
            throw new ModelAlreadyExistsError(this._id, this._sk)
          } else if (isConditionalCheckFailure && this.__src.isUpdate) {
            throw new InvalidModelUpdateError(this.id, this._sk)
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
   * [Model Foo:partitionKey:sortKey], so that each model has a unique
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
    return callback(first, params)
  } else {
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
   * Commits batched writes by sending DynamoDB requests.
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
      this.__extractError(request, response)
    })
    await request.promise()
    return true
  }

  /**
   * Find a model with the same TableName and Key from a list of models
   * @param {String} tableName
   * @param {Object} key { _id: '', _sk: '' }
   */
  __getModel (tableName, key) {
    const id = Object.values(key._id)[0]
    const sk = key._sk ? Object.values(key._sk)[0] : undefined
    let ret
    for (const model of this.__allModels) {
      if (model.__fullTableName === tableName &&
          model._id === id &&
          model._sk === sk) {
        ret = model
        break
      }
    }
    return ret
  }

  __extractError (request, response) {
    const responseBody = response.httpResponse.body.toString()
    const reasons = JSON.parse(responseBody).CancellationReasons
    for (let idx = 0; idx < reasons.length; idx++) {
      const reason = reasons[idx]
      if (reason.Code === 'ConditionalCheckFailed') {
        // Items in reasons maps 1to1 to items in request, here we do a reverse
        // lookup to find the original model that triggered the error.
        const trasact = request.params.TransactItems[idx]
        const method = Object.keys(trasact)[0]
        let model

        switch (method) {
          case 'Update':
          case 'ConditionCheck':
            model = this.__getModel(
              trasact[method].TableName,
              trasact[method].Key
            )
            break
          case 'Put':
            model = this.__getModel(
              trasact[method].TableName,
              trasact[method].Item
            )
            break
        }
        if (model.__src.isCreate) {
          response.error = new ModelAlreadyExistsError(model._id, model._sk)
        } else if (model.__src.isUpdate) {
          response.error = new InvalidModelUpdateError(model._id, model._sk)
        }
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
    this.emitter = new EventEmitter()

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
   * POST_COMMIT: When a transaction is commited. Do clean up,
   *              summery, post process here. Handler has the signature of
   *              (error) => {}. When error is undefined, the transaction
   *              commited successfully, else the transaction failed.
   */
  static EVENTS = {
    POST_COMMIT: 'postCommit'
  }

  /**
   * Add a handler to a specific event. See {@link EVENTS} for valid events.
   *
   * @param {Event} event A event to trigger the handler.
   * @param {Function} handler A handler for the event.
   */
  addHandler (event, handler) {
    if (!Object.values(Transaction.EVENTS).includes(event)) {
      throw new InvalidParameterError(event,
        `must be one of ${Object.values(Transaction.EVENTS)}`)
    }
    return this.emitter.once(event, handler)
  }

  /**
   * Get an item using DynamodDB's getItem API.
   *
   * @param {Key} key A key for the item
   * @param {GetParams} params Params for how to get the item
   */
  async __getItem (key, params) {
    params = params || {}
    if (!params.createIfMissing && Object.keys(key.data).length) {
      throw new InvalidParameterError('key()',
        'may only pass non-key fields to key() when createIfMissing is true')
    }
    const getParams = key.Cls.__getParams(key.encodedKeys, params)
    const data = await this.documentClient.get(getParams).promise()
    if (!params.createIfMissing && !data.Item) {
      return undefined
    }
    const isNew = !data.Item
    const vals = data.Item || key.vals
    const model = new key.Cls(ITEM_SOURCE.GET, isNew, vals)
    this.__writeBatcher.track(model)
    return model
  }

  /**
   * Gets multiple items using DynamoDB's transactGetItems API.
   * @param {Array<Key>} keys A list of keys to get.
   * @param {GetParams} params Params used to get items, all items will be
   *   fetched using the same params.
   */
  async __trasactGetItems (keys, params) {
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
    }).promise()
    const resps = data.Responses
    const models = []
    for (let idx = 0; idx < keys.length; idx++) {
      const data = resps[idx]
      if ((!params || !params.createIfMissing) && !data.Item) {
        models[idx] = undefined
        continue
      }
      const key = keys[idx]
      const model = new key.Cls(
        ITEM_SOURCE.GET,
        !data.Item,
        data.Item || key.vals)
      models[idx] = model
      this.__writeBatcher.track(model)
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
      }).promise()

      // Merge results
      const resps = data.Responses
      for (const [modelClsName, items] of Object.entries(resps)) {
        const Cls = modelClsLookup[modelClsName]
        for (const item of items) {
          unorderedModels.push(new Cls(ITEM_SOURCE.GET, false, item))
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
   *   get(Cls, keyValues, params)
   *   get(Key, params)
   *   get([Key], params)
   *
   * When only one items is fetched, DynamoDB's getItem API is called.
   *
   * When a list of items is fetched:
   *   If inconsistentRead is false (the default), DynamoDB's transactGetItems
   *     API is called for a strongly consistent read. Trasactional reads will
   *     be slower than batched reads.
   *   If inconsistentRead is true, DynamoDB's batchGetItems API is called.
   *     Batched fetches are more efficient than calling get with 1 key many
   *     times, since there is less HTTP request overhead. Batched fetches is
   *     faster than transactional fetches, but provides a weaker consistency.
   *
   * @param {Model} Cls a Model class.
   * @param {String|CompositeID} key Key or keyValues
   * @param {GetParams} [params]
   * @returns Model(s) associated with provided key
   */
  async get (...args) {
    return getWithArgs(args, async (arg, params) => {
      if (arg instanceof Array) {
        if (!params || !params.inconsistentRead) {
          return this.__trasactGetItems(arg, params)
        } else {
          return this.__batchGetItems(arg, params)
        }
      } else {
        return this.__getItem(arg, params)
      }
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
      if (Cls.__VIS_ATTRS[key].keyType !== undefined) {
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
    // We create the item we intend to write (with newData), and the update its
    // __initialValue for any preconditions requested (with `original`).
    // Creating the model with newData validates that newData specifies a
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

  /*
   * Runs a function in trasaction, emits events based on the execution outcome
   */
  async run (...args) {
    try {
      const ret = await this.__run(...args)
      this.emitter.emit(this.constructor.EVENTS.POST_COMMIT)
      return ret
    } catch (e) {
      this.emitter.emit(this.constructor.EVENTS.POST_COMMIT, e)
      throw e
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
        return new Transaction({}).run(args[0])
      case 2:
        return new Transaction(args[0]).run(args[1])
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
    ModelAlreadyExistsError,
    InvalidModelUpdateError
  }

  const toExport = Object.assign({}, exportAsClass)
  if (inDebugger) {
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
      ]
    }
  }
  return toExport
}
module.exports = setup()
