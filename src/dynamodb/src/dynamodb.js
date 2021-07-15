const assert = require('assert')

const deepeq = require('deep-equal')
const deepcopy = require('rfdc')()

const S = require('../../schema/src/schema')

const AsyncEmitter = require('./async-emitter')

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
 * @arg {string} msg the error message
 * @arg {Error} [originalException] the original error which led to this
 * @access public
 * @memberof Errors
 */
class TransactionFailedError extends Error {
  constructor (msg, originalException) {
    super(msg)
    this.name = this.constructor.name
    this.original = originalException
    if (originalException instanceof Error) {
      this.stack += '\n' + originalException.stack
    }
  }
}

/**
 * Thrown when there's some error with a particular model.
 * @memberof Errors
 */
class GenericModelError extends Error {
  constructor (msg, table, _id, _sk) {
    const skStr = (_sk !== undefined) ? ` _sk=${_sk}` : ''
    super(`${msg}: ${table} _id=${_id}${skStr}`)
    this.name = this.constructor.name
    this.retryable = false
  }
}

/**
 * Thrown when a model is to be created, but DB already has an item with the
 * same key.
 * @memberof Errors
 */
class ModelAlreadyExistsError extends GenericModelError {
  constructor (table, _id, _sk) {
    super('Tried to recreate an existing model', table, _id, _sk)
  }
}

/**
 * Thrown when a model is to be updated, but condition check failed.
 * @memberof Errors
 */
class InvalidModelUpdateError extends GenericModelError {
  constructor (table, _id, _sk) {
    super('Tried to update model with outdated / invalid conditions',
      table, _id, _sk)
  }
}

/**
 * Thrown when a model is to be deleted, but condition check failed.
 * @memberof Errors
 */
class InvalidModelDeletionError extends GenericModelError {
  constructor (table, _id, _sk) {
    super('Tried to delete model with outdated / invalid conditions',
      table, _id, _sk)
  }
}

/**
 * Thrown when an attempt to get a model that is deleted or created in a
 * transaction where cachedModels option is on.
 * @memberof Errors
 */
class InvalidCachedModelError extends GenericModelError {
  constructor (model) {
    super('Model is not a valid cached model',
      model.constructor.fullTableName, model._id, model._sk)
  }
}

/**
 * Thrown when a model is being created more than once.
 * @memberof Errors
 */
class ModelCreatedTwiceError extends GenericModelError {
  constructor (model) {
    super('Tried to create model when it\'s already created in the same tx',
      model.__fullTableName, model._id, model._sk)
    this.model = model
  }
}

/**
 * Thrown when a model is being deleted more than once.
 * @memberof Errors
 */
class ModelDeletedTwiceError extends GenericModelError {
  constructor (model) {
    super('Tried to delete model when it\'s already deleted in the current tx',
      model.__fullTableName, model._id, model._sk)
    this.model = model
  }
}

/**
 * Thrown when a tx tries to write when it was marked read-only.
 * @memberof Errors
 */
class WriteAttemptedInReadOnlyTxError extends Error {
  constructor (table, _id, _sk) {
    super('Tried to write model in a read-only transaction', table, _id, _sk)
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
  // istanbul ignore next
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
  static __validateFieldOptions (modelName, keyType, fieldName, schema) {
    if (fieldName.startsWith('_')) {
      throw new InvalidFieldError(
        fieldName, 'property names may not start with "_"')
    }

    assert(['PARTITION', 'SORT', undefined].includes(keyType),
      'keyType must be one of \'PARTITION\', \'SORT\' or undefined')
    assert(schema.isTodeaSchema, 'must be Todea schema')
    const compiledSchema = schema.getValidatorAndJSONSchema(
      `${modelName}.${fieldName}`)
    schema = compiledSchema.jsonSchema
    const isKey = !!keyType
    const options = {
      keyType,
      schema,
      optional: schema.optional === true,
      immutable: isKey || schema.readOnly === true,
      default: schema.default,
      validateOrDie: compiledSchema.validateOrDie
    }
    const FieldCls = schemaTypeToFieldClassMap[options.schema.type]
    assert.ok(FieldCls, `unsupported field type ${options.schema.type}`)

    const hasDefault = Object.prototype.hasOwnProperty.call(schema, 'default')
    if (hasDefault && options.default === undefined) {
      throw new InvalidFieldError(fieldName,
        'the default value cannot be set to undefined')
    }
    if (isKey) {
      if (hasDefault && keyType === 'PARTITION') {
        throw new InvalidOptionsError('default',
          'No defaults for partition keys.') // It just doesn\'t make sense.
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
    if (hasDefault) {
      validateValue(this.name, options, options.default)
    }
    return options
  }

  /**
   * @typedef {Object} FieldOptions
   * @property {'PARTITION'|'SORT'} [keyType=undefined] If specified, the field is
   *   a key. Use 'PARTITION' for a partition key. Use 'SORT' for a sort key.
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
   * @param {String} name the field's name (also the name of the underlying
   *   attribute in the database where this field is stored [except key
   *   components which are not stored in the db in their own attribute])
   * @param {FieldOptions} opts
   * @param {*} val the initial value of the field
   * @param {boolean} valIsFromDB whether val is from (or is expected to be
   *   from) the database
   * @param {boolean} valSpecified whether val was specified (if this is
   *   true, then the field was present)
   * @param {boolean} isForUpdate whether this field is part of an update
   * @param {boolean} isForUpdate whether this field is part of an delete
   */
  constructor ({
    idx,
    name,
    opts,
    val,
    valIsFromDB,
    valSpecified,
    isForUpdate,
    isForDelete
  }) {
    for (const [key, value] of Object.entries(opts)) {
      Object.defineProperty(this, key, { value, writable: false })
    }

    // Setup states
    /**
     * @memberof Internal.__Field
     * @instance
     * @member {String} name The name of the owning property.
     */
    this.__idx = idx
    this.name = name
    this.__value = undefined
    this.__readInitialValue = false // If get is called
    this.__written = false // If set is called
    this.__default = opts.default // only used for new items!

    // determine whether to use the default value, or the given val
    let useDefault
    if (valSpecified) {
      // if val was specified, then use it
      useDefault = false
    } else {
      assert.ok(val === undefined,
        'valSpecified can only be false if val is undefined')
      if (this.__default === undefined) {
        // can't use the default if there is no default value
        useDefault = false
      } else if (valIsFromDB && this.optional) {
        // if the field is optional and the value is not in the db then we
        // can't use the default (we have to assume the field was omitted)
        useDefault = false
      } else if (isForUpdate) {
        // when creating an item as the base of an update, we don't implicitly
        // create defaults; our preconditions are ONLY what is explicitly given
        useDefault = false
      } else {
        useDefault = true
      }
    }

    this.__value = useDefault ? deepcopy(this.__default) : val

    if (valIsFromDB) {
      // The field's current value is the value stored in the database. Track
      // that value so that we can detect if it changes, and write that
      // change to the database.
      // Note: val is undefined whenever useDefault is true
      this.__initialValue = deepcopy(val)
    } else {
      this.__initialValue = undefined
    }

    // validate the value, if needed
    if (!this.keyType) { // keys are validated elsewhere; don't re-validate
      if (!useDefault) { // default was validated by __validateFieldOptions
        // validate everything except values omitted from an update() call
        if (valSpecified || !(isForUpdate || isForDelete)) {
          this.validate()
        }
      }
    }
  }

  /**
   * Name used in AWS expressions.
   *
   * This name is short for performance reasons, and is used with
   * ExpressionAttributeNames to avoid collisions with reserved AWS names.
   */
  get __awsName () {
    return `#${this.__idx}`
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
          `${this.__awsName}=${exprKey}`,
          { [exprKey]: deepcopy(this.__value) },
          false
        ]
      }
    }
    return []
  }

  /**
   * Whether to condition the transaction on this field's initial value.
   */
  get canUpdateWithoutCondition () {
    return (
      // keys uniquely identify an item; all keys generate a condition check
      this.keyType === undefined &&
      // if an item's value is read before it is modified, then we must verify
      // that it's value doesn't change
      !this.__readInitialValue)
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
    if (this.canUpdateWithoutCondition) {
      return []
    }
    if (this.__initialValue === undefined) {
      return [
        `attribute_not_exists(${this.__awsName})`,
        {}
      ]
    }
    return [
      `${this.__awsName}=${exprKey}`,
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
    return this.__readInitialValue || this.__written
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
    if (!this.__written) {
      this.__readInitialValue = true
    }
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
  opts.validateOrDie(val)
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
    this.__mustUseSet = false

    // figure out what value the diff will be added to
    if (this.__initialValue !== undefined) {
      // this occurs when the field is on an existing item AND the item already
      // had a value for the field
      this.__base = this.__initialValue
    } else if (this.__value !== undefined) {
      // this case occurs when the field is on a new item
      this.__base = this.__value
    } else {
      // this case occurs if the field is not currently present on the item;
      // in this case increment cannot be used to update the item
      this.__base = undefined
    }
  }

  set (val) {
    super.set(val)
    // don't change any state unless set() succeeds
    this.__diff = undefined // no longer computed as a diff
    this.__mustUseSet = true
  }

  /**
   * Updates the field's value by an unconditioned increment IF the field is
   * never read (reduces contention). If the field is ever read, there's no
   * reason to use this.
   * @param {Number} diff The diff amount.
   */
  incrementBy (diff) {
    // add the new diff to our current diff, if any
    // wait to set __diff until after super.set() succeeds to ensure no
    // changes are made if set() fails!
    const newDiff = (this.__diff === undefined) ? diff : this.__diff + diff

    // if we've already read the value, there's no point in generating an
    // increment update expression as we must lock on the original value anyway
    if (this.__readInitialValue || this.__mustUseSet) {
      this.set(this.__sumIfValid(false, newDiff))
      // this.__diff isn't set because we can't not using diff updates now
      return
    }

    // call directly on super to avoid clearing the diff value
    super.set(this.__sumIfValid(true, newDiff))
    this.__diff = newDiff
  }

  /**
   * Returns the sum of diff and some value. Throws if the latter is undefined.
   * @param {boolean} fromBase whether to add __base (else __value)
   * @param {number} diff how much to add
   * @returns {number} sum
   */
  __sumIfValid (fromBase, diff) {
    const base = fromBase ? this.__base : this.__value
    if (base === undefined) {
      throw new InvalidFieldError(
        this.name, 'cannot increment a field whose value is undefined')
    }
    return base + diff
  }

  /**
   * Whether this field can be updated with an increment expression.
   */
  get canUpdateWithIncrement () {
    return (
      // if there's no diff, we cannot use increment
      this.__diff !== undefined &&
      // if the field didn't have an old value, we can't increment it (DynamoDB
      // will throw an error if we try to do X=X+1 when X has no value)
      this.__initialValue !== undefined &&
      // if we're generating a condition on the initial value, there's no
      // benefit to do an increment so we can just do a standard set
      this.canUpdateWithoutCondition)
  }

  __updateExpression (exprKey) {
    // if we're locking, there's no point in doing an increment
    if (this.canUpdateWithIncrement) {
      return [
        `${this.__awsName}=${this.__awsName}+${exprKey}`,
        { [exprKey]: this.__diff },
        false
      ]
    }
    return super.__updateExpression(exprKey)
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
 * Key uniquely identifies a model.
 */
class Key {
  /**
   * @param {Model} Cls a Model class
   * @param {Object} encodedKeys map of encoded partition and sort key
   * @param {Object} keyComponents key component values
   * @private
   */
  constructor (Cls, encodedKeys, keyComponents) {
    this.Cls = Cls
    this.encodedKeys = encodedKeys
    this.keyComponents = keyComponents
  }
}

/**
 * Data includes a model's key and non-key fields.
 * @param {Object} [fields] field (non-key) values
 */
class Data extends Key {
  constructor (Cls, encodedKeys, keyComponents, fields) {
    super(Cls, encodedKeys, keyComponents)
    this.data = fields
  }

  get key () {
    return new Key(this.Cls, this.encodedKeys, this.keyComponents)
  }

  get vals () {
    return { ...this.keyComponents, ...this.data }
  }
}

// sentinel values for different item creation methods
const ITEM_SOURCE = {
  CREATE: { isCreate: true },
  CREATE_OR_PUT: { isCreateOrPut: true },
  DELETE: { isDelete: true }, // Delete by key creates a local model
  GET: { isGet: true },
  SCAN: { isScan: true },
  UPDATE: { isUpdate: true }
}
const ITEM_SOURCES = new Set(Object.values(ITEM_SOURCE))

/**
 * The base class for modeling data.
 */
class Model {
  /**
   * Create a representation of a database Item. Should only be used by the
   * library.
   */
  constructor (src, isNew, vals) {
    this.isNew = !!isNew

    if (!ITEM_SOURCES.has(src)) {
      throw new InvalidParameterError('src', 'invalid item source type')
    }
    this.__src = src

    // track whether this item has been written to the db yet
    this.__written = false

    // track whether this item has been marked for deletion
    this.__toBeDeleted = src.isDelete

    // __attrs has a __Field subclass object for each non-key attribute.
    this.__attrs = {}

    // Decode _id and _sk that are stored in DB into key components that are
    // in KEY and SORT_KEY.
    const setupKey = (attrName, keyOrder, vals) => {
      const attrVal = vals[attrName]
      if (attrVal === undefined) {
        return
      }

      delete vals[attrName]
      Object.assign(vals, this.constructor.__decodeCompoundValueFromString(
        keyOrder, attrVal, attrName))
    }
    setupKey('_id', this.constructor.__keyOrder.partition, vals)
    setupKey('_sk', this.constructor.__keyOrder.sort, vals)

    // add user-defined fields from FIELDS & key components from KEY & SORT_KEY
    let fieldIdx = 0
    for (const [name, opts] of Object.entries(this.constructor._attrs)) {
      this.__addField(fieldIdx++, name, opts, vals)
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
  __finalize () {
  }

  __addField (idx, name, opts, vals) {
    const val = vals[name]
    const valSpecified = Object.hasOwnProperty.call(vals, name)
    const Cls = schemaTypeToFieldClassMap[opts.schema.type]
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
    this.__attrs[name] = field
    Object.defineProperty(this, name, {
      get: (...args) => {
        return field.get()
      },
      set: (val) => {
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
      if (schema.optional) {
        throw new GenericModelError(
          'EXPIRE_EPOCH_FIELD must refer to a required field',
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

    // _attrs maps the name of attributes that are visible to users of
    // this model. This is the combination of attributes (keys) defined by KEY,
    // SORT_KEY and FIELDS.
    this._attrs = {}
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
  }

  static __getResourceDefinition () {
    this.__doOneTimeModelPrep()
    // the partition key attribute is always "_id" and of type string
    const attrs = [{ AttributeName: '_id', AttributeType: 'S' }]
    const keys = [{ AttributeName: '_id', KeyType: 'HASH' }]

    // if we have a sort key attribute, it always "_sk" and of type string
    if (this.__keyOrder.sort.length > 0) {
      attrs.push({ AttributeName: '_sk', AttributeType: 'S' })
      keys.push({ AttributeName: '_sk', KeyType: 'RANGE' })
    }

    const ret = {
      TableName: this.fullTableName,
      AttributeDefinitions: attrs,
      KeySchema: keys,
      BillingMode: 'PAY_PER_REQUEST'
    }

    if (this.EXPIRE_EPOCH_FIELD) {
      ret.TimeToLiveSpecification = {
        AttributeName: this.EXPIRE_EPOCH_FIELD,
        Enabled: true
      }
    }
    return ret
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

  get _id () {
    return this.__getKey(this.constructor.__keyOrder.partition)
  }

  get _sk () {
    return this.__getKey(this.constructor.__keyOrder.sort)
  }

  __getKey (keyOrder) {
    return this.constructor.__encodeCompoundValueToString(
      keyOrder, new Proxy(this, {
        get: (target, prop, receiver) => {
          return target.getField(prop).__value
        }
      })
    )
  }

  get __encodedKey () {
    const ret = {
      _id: this._id
    }
    const sk = this._sk
    if (sk) {
      ret._sk = sk
    }
    return ret
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
    const keyAttrs = {
      _id: this.__encodeCompoundValueToString(this.__keyOrder.partition, vals)
    }

    // add and validate the sort attribute, if any
    if (this.__keyOrder.sort.length > 0) {
      keyAttrs._sk = this.__encodeCompoundValueToString(
        this.__keyOrder.sort, vals
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
    assert(!name.startsWith('_'), 'may not access internal computed fields')
    return this.__attrs[name]
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
    for (const [key, field] of Object.entries(this.__attrs)) {
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
      const conditions = []
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
    const exprAttrNames = {}
    const exprValues = {}
    const itemKey = this.__encodedKey
    const sets = []
    const removes = []
    const accessedFields = []
    let exprCount = 0

    const isUpdate = this.__src.isUpdate
    for (const field of Object.values(this.__attrs)) {
      const omitInUpdate = isUpdate && field.get() === undefined
      const doValidate = (shouldValidate === undefined || shouldValidate) &&
        !omitInUpdate
      if (doValidate) {
        field.validate()
      }

      if (field.keyType) {
        continue
      }

      if (omitInUpdate) {
        // When init method is UPDATE, not all required fields are present in the
        // model: we only write parts of the model.
        // Hence we exclude any fields that are not part of the update.
        continue
      }

      const exprKey = `:_${exprCount++}`
      const [set, vals, remove] = field.__updateExpression(exprKey)
      if (set) {
        sets.push(set)
        Object.assign(exprValues, vals)
      }
      if (remove) {
        removes.push(field.__awsName)
      }
      if (field.accessed) {
        accessedFields.push(field)
      }
      if (set || remove) {
        exprAttrNames[field.__awsName] = field.name
      }
    }

    if (this.isNew) {
      const [cond, names, vals] = this.__nonexistentModelCondition()
      conditions.push(cond)
      Object.assign(exprAttrNames, names)
      Object.assign(exprValues, vals)
    } else {
      if (isUpdate) {
        conditions.push('attribute_exists(#_id)')
        exprAttrNames['#_id'] = '_id'
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
      const conditions = [
        'attribute_exists(#_id)'
      ]
      const attrNames = {
        '#_id': '_id'
      }
      const conditionCheckParams = this.__updateParams(false)
      if (conditionCheckParams.ConditionExpression) {
        conditions.push(conditionCheckParams.ConditionExpression)
        Object.assign(attrNames, conditionCheckParams.ExpressionAttributeNames)
        ret.ExpressionAttributeValues =
          conditionCheckParams.ExpressionAttributeValues
      }

      ret.ConditionExpression = conditions.join(' AND ')
      ret.ExpressionAttributeNames = attrNames
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
    return this.isNew ||
      this.__toBeDeleted ||
      Object.values(this.__attrs).reduce(
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
    if (keyOrder.length === 0) {
      return undefined
    }
    const pieces = []
    for (var i = 0; i < keyOrder.length; i++) {
      const fieldName = keyOrder[i]
      const fieldOpts = this._attrs[fieldName]
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
      const fieldOpts = this._attrs[fieldName]
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
    assert.ok(this.__keyOrder,
      `model ${this.name} one-time setup was not done (remember to export ` +
      'the model and in unit tests remember to call createResource()')
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
        await this.documentClient[method](params).promise()
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
        (attribute_exists(${awsName}) and
         :_ttlMin <= ${awsName} and
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

  /**
   * Return snapshot of the model, all fields included.
   * @param {Object} params
   * @param {Boolean} params.initial Whether to return the initial state
   * @param {Boolean} params.dbKeys Whether to return _id and _sk instead of
   *   raw key fields.
   */
  getSnapshot ({ initial = false, dbKeys = false }) {
    if (initial === false && this.__toBeDeleted) {
      return {}
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
    for (const [name, field] of Object.entries(this.__attrs)) {
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
}

/**
 * Used for tracking a non-existent item.
 */
class NonExistentItem {
  constructor (key) {
    this.key = key
  }

  __isMutated () {
    return false
  }

  __conditionCheckParams () {
    const key = this.key
    const model = new key.Cls(ITEM_SOURCE.GET, true, key.keyComponents)
    const [
      condition, attrNames, attrValues
    ] = model.__nonexistentModelCondition()
    return {
      TableName: this.key.Cls.fullTableName,
      Key: this.key.encodedKeys,
      ConditionExpression: condition,
      ExpressionAttributeNames: attrNames,
      ExpressionAttributeValues: attrValues
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
}

/**
 * DataBase iterator. Supports query and scan operations.
 * @private
 */
class __DBIterator {
  static OPERATION_NAME = undefined

  constructor ({
    Cls,
    writeBatcher,
    options
  }) {
    const {
      inconsistentRead = false
    } = options || {}

    this.__writeBatcher = writeBatcher
    this.__ModelCls = Cls
    this.__fetchParams = undefined
    this.inconsistentRead = inconsistentRead
  }

  __setupParams () {
    if (!this.__fetchParams) {
      const params = {
        TableName: this.__ModelCls.fullTableName,
        ConsistentRead: !this.inconsistentRead
      }
      this.__fetchParams = params
    }
    return this.__fetchParams
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
   * @return {Tuple(Array<Model>, String)} A tuple of (items, nextToken). When nextToken
   *   is undefined, the end of the DB table has been reached.
   */
  async __getBatch (n, nextToken = undefined) {
    this.__setupParams()

    const params = this.__fetchParams
    params.Limit = n
    if (!nextToken) {
      delete params.ExclusiveStartKey
    } else {
      params.ExclusiveStartKey = nextToken
    }
    const op = this.constructor.OPERATION_NAME
    const result = await this.documentClient[op](this.__fetchParams).promise()

    const models = result.Items.map(item => {
      const m = new this.__ModelCls(ITEM_SOURCE.SCAN, false, item)
      if (m.__hasExpired) {
        return undefined
      }
      this.__writeBatcher.track(m)
      return m
    }).filter(m => !!m)

    return [
      models,
      result.LastEvaluatedKey
    ]
  }

  /**
   * Fetch n items from DB, return the fetched items and a token to next page.
   *
   * @param {Integer} n The number of items to return.
   * @param {Object} [nextToken=undefined] A token for fetching the next batch.
   *   It is returned from a previous call to fetch. When nextToken is
   *   undefined, the function will go from the start of the DB table.
   *
   * @return {Tuple(Array<Model>, String)} A tuple of (items, nextToken). When nextToken
   *   is undefined, the end of the DB table has been reached.
   */
  async fetch (n, nextToken = undefined) {
    const ret = []
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
    return [ret, nextToken]
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
  static OPERATION_NAME = 'scan'
}

/**
 * Returns a string which uniquely identifies an item.
 * @param {Model} modelCls the Model for the item
 * @param {string} _id the item's partition key
 * @param {string} [_sk] the item's sort key
 */
function makeItemString (modelCls, _id, _sk) {
  const arr = [modelCls.tableName, _id]
  if (_sk !== undefined) {
    arr.push(_sk)
  }
  return JSON.stringify(arr)
}

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
    return request.promise()
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
    }).promise()
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
      }).promise()

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
        await this.__eventEmitter.emit(this.constructor.EVENTS.FAILURE, err)
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
