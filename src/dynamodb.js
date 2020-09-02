const ajv = new (require('ajv'))({
  allErrors: true,
  removeAdditional: 'failing'
})
const assert = require('assert')
const deepeq = require('deep-equal')
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
 * same ID.
 */
class ModelAlreadyExistsError extends Error {
  constructor (id) {
    super(`Tried to recreate an existing model: ${id}`)
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
 */

/**
 * @namespace Internal
 */

/**
 * Internal object representing a field / property of a Model.
 *
 * @access package
 * @memberof Internal
 */
class __Field {
  /**
   * Returns supported options, and their default values.
   */
  get defaultOptions () {
    return {
      keyType: undefined,
      optional: false,
      immutable: false,
      default: undefined,
      schema: undefined
    }
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
   * @property {fluent-schema} [schema=undefined] An optional fluent schema
   *   to validate Field's value. Field class obviously already constraints the
   *   root object's type, but schema option can be used to add additional
   *   constraints. For example, limiting StringField's values min and max
   *   length. Or for ArrayField, shape of the underlying data can be better
   *   defined with schema.
   *   Noteworthily, Field's optional option superceeds schema's required
   *   property, meaning if an optional StringField's schema requires a string,
   *   overall the Feild is not required, and vice versa.
   */

  /**
   * @param {FieldOptions} [options]
   */
  constructor (options) {
    // Validate options
    options = Object.assign({}, options) // Copy so we own options now
    const defaults = this.defaultOptions
    checkUnexpectedOptions(options, defaults)

    const validKeyTypes = [undefined, 'HASH', 'RANGE']
    if (!validKeyTypes.includes(options.keyType)) {
      throw new InvalidOptionsError('keyType',
        `Invalid value ${options.keyType}. Valid values are ${validKeyTypes}`)
    }

    // Setup options
    const hasDefault = Object.prototype.hasOwnProperty.call(options, 'default')
    if (options.keyType !== undefined) {
      if (hasDefault) {
        throw new InvalidOptionsError('default',
          'No defaults for keys. It just doesn\'t make sense.')
      }
      if (options.immutable !== undefined && !options.immutable) {
        throw new InvalidOptionsError('immutable',
          'Keys must be immutable.')
      }
      if (options.optional) {
        throw new InvalidOptionsError('optional',
          'Keys must never be optional.')
      }
      options.immutable = true
      options.optional = false
    }

    if (options.schema) {
      assert.ok(options.schema.isFluentSchema)
      options.schema = options.schema.valueOf()

      if (schemaTypeToJSTypeMap[options.schema.type] !== this.valueType) {
        throw new InvalidOptionsError('schema',
          `Incompatible schema type ${options.schema.type} and field type ` +
          `${this.valueType}.`
        )
      }

      options.schemaValidator = compileSchema(options.schema)
    }

    options = Object.assign(defaults, options)

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
    if (hasDefault) {
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
   * Returns a class name indicating the underlying value's type, e.g. Number
   *
   * @abstract
   */
  get valueType () {
    throw new Error('Needs to be overridden')
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
   * @returns {Boolean} true if valid.
   */
  validate () {
    const val = this.__value
    if (!this.optional && val === undefined) {
      throw new InvalidFieldError(
        this.name,
        'is not optional and is unset')
    }
    validateValue(this, val)
    return true
  }
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
function validateValue (field, val) {
  if (val !== undefined && val.constructor.name !== field.valueType.name) {
    throw new InvalidFieldError(
      this.name,
      `value ${val} is not type ${field.valueType.name}`)
  }

  if (field.schemaValidator && !field.schemaValidator(val)) {
    throw new InvalidFieldError(
      field.name,
      `value ${JSON.stringify(val)} does not conform to schema ` +
      `${JSON.stringify(field.schema)} with error ` +
      JSON.stringify(field.schemaValidator.errors, null, 2)
    )
  }
}

/**
 * Return the compiled schema.
 * @param {JsonSChema} schema the JSON schema to compile; if it contains the
 *   key "required" it will be removed unless the schema is of type "object"
 */
function compileSchema (schema) {
  if (schema.type !== 'object' && schema.required) {
    delete schema.required
  }
  return ajv.compile(schema)
}

/**
 * @public
 * @extends Internal.__Field
 * @memberof Fields
 */
class NumberField extends __Field {
  get valueType () {
    return Number
  }

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
 * @public
 * @extends Internal.__Field
 * @memberof Fields
 */
class StringField extends __Field {
  get valueType () {
    return String
  }
}

/**
 * @public
 * @extends Internal.__Field
 * @memberof Fields
 */
class ObjectField extends __Field {
  get valueType () {
    return Object
  }

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
 * @public
 * @extends Internal.__Field
 * @memberof Fields
 */
class BooleanField extends __Field {
  get valueType () {
    return Boolean
  }
}

/**
 * @public
 * @extends Internal.__Field
 * @memberof Fields
 */
class ArrayField extends __Field {
  get valueType () {
    return Array
  }

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
 * Key object to identify models.
 * @example
 * Key(SomeModel, 'someid') // No range keys
 * Key(SomeOtherModel, { id: 'someOtherID', someRangeKey: 'range' })
 */
class Key {
  /**
   * @param {Model} Cls a Model class
   * @param {String|CompositeID} id unique identifier for an item
   */
  constructor (Cls, id) {
    if (arguments.length > 2) {
      throw new InvalidParameterError('extra params', 'Expects Cls and id')
    }
    // id validation is coupled with Model instances. Instead of instantiating
    // a model here for validating id, just do minimal checks here and have the
    // library throw when the key is fetched.
    if (!id) {
      throw new InvalidParameterError('id', 'Expecting an id')
    }
    if (!(Cls.prototype instanceof Model)) {
      throw new InvalidParameterError('Cls',
        'Model class must be a subclass of db.Model')
    }

    if (id instanceof Object && Cls.__ID_SCHEMA) {
      // this validates the ID too
      id = { id: Cls.__ID_SCHEMA.compoundValueToString(id) }
    } else {
      if (typeof id === 'string') {
        id = { id }
      }
      Cls.__checkValidId(id.id)
    }
    this.Cls = Cls
    this.compositeID = deepcopy(id)
  }
}

/**
 * Describes a field that may be composed of one or more components.
 *
 * Can be serialized to and from a compact string format.
 * @private
 */
class CompoundValueSchema {
  /**
   * Create an empty CompoundValueSchema.
   */
  constructor () {
    this.components = []
  }

  /**
   * Like {@link CompoundValueSchema.component} but adds an additional
   * component to the schema (so that schema will have >1 components, and thus
   * describe a compound value).
   */
  addComponent (name, schema) {
    if (this.components.length) {
      assert.ok(name !== 'id' && this.components[0].name !== 'id',
        'id cannot be the name of a component in a compound schema')
    }
    assert.ok(schema.isFluentSchema)
    schema = schema.valueOf()
    assert.ok(name !== 'id' || schema.type === 'string',
      'a schema for an ID field named "id" must be a string type')
    const valueType = schemaTypeToJSTypeMap[schema.type]
    const schemaValidator = compileSchema(schema)
    // these keys mirror the names used by Field so they can share the
    // validateSchema() function
    this.components.push({ name, schema, schemaValidator, valueType })
    return this
  }

  /**
   * Returns the string ID which corresponds to the specified compound ID.
   *
   * This method throws {@link InvalidFieldError} if the compound ID does not
   * match the required schema.
   *
   * @param {Object} compoundID maps component names to values
   */
  compoundValueToString (compoundID) {
    const componentNames = Object.keys(compoundID)
    if (componentNames.length !== this.components.length) {
      throw new InvalidFieldError(
        'id', 'invalid number of components for this compound ID')
    }
    const pieces = []
    for (var i = 0; i < this.components.length; i++) {
      const component = this.components[i]
      const givenValue = compoundID[component.name]
      if (givenValue === undefined) {
        throw new InvalidFieldError(
          'id', `missing component ${component.name}`)
      }
      validateValue(component, givenValue)
      if (component.valueType === String) {
        // to workaround this, put them inside an object
        if (givenValue.indexOf('\0') !== -1) {
          throw new InvalidFieldError(
            'id', 'cannot put null bytes in strings in compound IDs')
        }
        pieces.push(givenValue)
      } else {
        pieces.push(JSON.stringify(givenValue))
      }
    }
    return pieces.join('\0')
  }

  /**
   * Returns the compound ID object which corresponds to the given string ID.
   *
   * This method throws {@link InvalidFieldError} if the string ID does not
   * match the required schema.
   *
   * @param {String} id the string representation of a compound ID
   */
  stringToCompoundValue (id) {
    const compoundID = {}
    const pieces = id.split('\0')
    if (pieces.length !== this.components.length) {
      throw new InvalidFieldError(
        'id', 'failed to parse ID: incorrect number of components')
    }
    for (let i = 0; i < pieces.length; i++) {
      const piece = pieces[i]
      const component = this.components[i]
      if (component.valueType === String) {
        compoundID[component.name] = piece
      } else {
        compoundID[component.name] = JSON.parse(piece)
      }
      validateValue(component, compoundID[component.name])
    }
    return compoundID
  }
}

/**
 * The base class for modeling data.
 * @public
 *
 * @property {StringField} id The partition key for a model.
 * @property {Boolean} isNew Whether the item exists on server.
 */
class Model {
  /**
   * Constructs a model. Model has one `id` field. Subclasses should add
   * additional fields by overriding the constructor.
   *
   * @param {GetParams} [params] Parameters used to fetch models from DB, or
   *   device any additional behavior at instantiation time. Unused internally
   *   here, but documented for the sake of subclassing.
   */
  constructor (params) {
    this.id = new StringField({ keyType: 'HASH' })
    this.isNew = false
    this.__fields = {}
    this.__written = false

    // add a getter for each component of the ID
    if (!this.constructor.__isSingularIDWithStandardName()) {
      const idSchema = this.constructor.__ID_SCHEMA
      const components = idSchema.components
      for (let i = 0; i < components.length; i++) {
        const component = components[i]
        // would be nice to check if there are collisions with the field names
        // but we can't since the subclass constructor hasn't finished yet
        Object.defineProperty(this, component.name, {
          get: () => this.__idComponents[component.name]
        })
      }
    }
  }

  /**
   * Returns whether we're using just the standard partition key ("id").
   *
   * Will return false if we have a custom name for the partition key, or if
   * the partition key is a compound value (i.e., it is composed of more than
   * one components).
   */
  static __isSingularIDWithStandardName () {
    const idSchema = this.__ID_SCHEMA
    return !idSchema || (
      idSchema.components.length === 1 &&
      idSchema.components[0].name === 'id')
  }

  /**
   * Sets the schema for the partition key.
   * @param {Map<String,fluent-schemna>|fluent-schema} idSchema describes the
   *   structure of the partition key for this model. May be a fluent-schema
   *   S.string(), or a map of component names to schemas.
   */
  static setSchemaForID (idSchema) {
    // though the underlying ID is always stored as a string in the database
    // (not strictly required; just a design choice this library made), it is
    // possible for that ID to be "compound" or composed of one or more parts,
    // each with their own schema
    assert.ok(!Object.hasOwnProperty.call(this, '__ID_SCHEMA'),
      'can only call setSchemaForID() once per model class')
    if (idSchema.isFluentSchema) {
      // the ID has just one component with the usual name "id"; the supplied
      // schema describes it; create the equivalent compound schema
      this.__ID_SCHEMA = new CompoundValueSchema().addComponent('id', idSchema)
    } else {
      const keys = Object.keys(idSchema)
      assert.ok(keys.length, 'the id field must have one or more components')
      keys.sort() // must always appear in the exact same order
      this.__ID_SCHEMA = new CompoundValueSchema()
      for (let i = 0; i < keys.length; i++) {
        const key = keys[i]
        this.__ID_SCHEMA.addComponent(key, idSchema[key])
      }
    }
  }

  /**
   * Returns the string ID which corresponds to the specified compound ID.
   * {@see CompoundValueSchema.compoundValueToString}
   * @param {Object} compoundID maps component names to values
   */
  static compoundValueToString (compoundID) {
    if (!this.__ID_SCHEMA) {
      return compoundID.id
    }
    return this.__ID_SCHEMA.compoundValueToString(compoundID)
  }

  /**
   * Verifies that the ID is valid (i.e., matches the required schema).
   * @param {String} id the string ID used as the partition key
   */
  static __checkValidId (id) {
    const valid = (typeof id === 'string' || id instanceof String) && id.length
    if (!valid) {
      throw new InvalidParameterError('id', 'Must be a non-empty string')
    }

    if (this.__ID_SCHEMA) {
      // asserts if the ID is malformed
      this.__ID_SCHEMA.stringToCompoundValue(id)
    }
  }

  /**
   * Retrieves a field to achieve more complex behaviors.
   *
   * @param {String} name Property name.
   * @returns {BooleanField|ArrayField|ObjectField|NumberField|StringField}
   */
  getField (name) {
    return this.__fields[name]
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
  get fullTableName () {
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
      const value = this.__fields[key] || this[key]
      if (value instanceof __Field &&
          value.keyType !== undefined) {
        compositeID[key] = data[key]
      } else {
        modelData[key] = data[key]
      }
    })
    this.__checkCompositeID(compositeID)
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
      TableName: this.fullTableName,
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
   * A composite ID uniquely identifies any model in a table, containing
   * a partition key and zero to many sort keys.
   * @typedef {Object} CompositeID
   * @property {String} id A partition key.
   * @property {*} [*] Any additional range / sort keys.
   */

  /**
   * Validates a CompositeID. Throws if invalid.
   *
   * @param {CompositeID} compositeID
   */
  __checkCompositeID (compositeID) {
    if (!compositeID ||
        compositeID.constructor !== Object ||
        Object.keys(compositeID).length === 0) {
      throw new InvalidParameterError('compositeID',
        'Must be an object containing at least a hash key. For example ' +
        '{ id: \'someid\' }')
    }
    Object.keys(this).forEach(key => {
      const value = this[key]
      if (value instanceof __Field &&
          value.keyType !== undefined &&
          !compositeID[key]) {
        throw new InvalidParameterError('compositeID',
          `missing value for key field ${key}.`)
      }
    })
    const nonIDs = Object.keys(compositeID).filter(key => {
      return !(this[key] instanceof __Field) || this[key].keyType === undefined
    })
    if (nonIDs.length !== 0) {
      throw new InvalidParameterError('compositeID', 'non key fields detected')
    }
    this.constructor.__checkValidId(compositeID.id)
    return true
  }

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
      // conditions.push('attribute_exists(id)')
      assert.fail('This should be unreachable unless something is broken.')
    }

    const item = {}
    const accessedFields = []
    let exprCount = 0
    Object.keys(this.__fields).forEach(key => {
      const field = this.__fields[key]
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
          conditionExpr = `attribute_not_exists(id) OR (${conditionExpr})`
        }
      } else {
        conditionExpr = 'attribute_not_exists(id)'
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
      TableName: this.fullTableName,
      Item: item
    }
    if (conditionExpr.length !== 0) {
      ret.ConditionExpression = conditionExpr
    }
    if (Object.keys(exprValues).length) {
      ret.ExpressionAttributeValues = exprValues
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

    Object.keys(this.__fields).forEach(key => {
      const field = this.__fields[key]
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

    if (this.isNew) {
      conditions.push('attribute_not_exists(id)')
    } else {
      if (isUpdate) {
        conditions.push('attribute_exists(id)')
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
      TableName: this.fullTableName,
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
    return ret
  }

  /**
   * Indicates if any field was mutated. New models are considered to be
   * mutated as well.
   *
   * @type {Boolean}
   */
  get mutated () {
    return this.isNew || Object.values(this.__fields).reduce(
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
    assert.ok(this.isNew || !this.mutated,
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
        this.__fields[key] = field
        field.__setup(vals[key])

        if (field.keyType !== undefined) {
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

    if (!this.constructor.__isSingularIDWithStandardName()) {
      const idSchema = this.constructor.__ID_SCHEMA
      this.__idComponents = idSchema.stringToCompoundValue(this.id)
    }

    // Once setup, restrict access to model.
    Object.seal(this)
  }

  /**
   * @param {String|CompositeID} id
   * @returns {Key} a Key object.
   */
  static key (id) {
    return new Key(this, id)
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
            throw new ModelAlreadyExistsError(this.id)
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
   * Shows the type and id of a model, for example, [Model Foo:someid], so that
   * each model has a unique identifier to be used in Object and Set.
   */
  toString () {
    return `[Model ${this.constructor.name}:${this.__fields.id.__value}]`
  }
}

async function getWithArgs (args, callback) {
  if (!args || !(args instanceof Array) || args.length === 0) {
    throw new InvalidParameterError('args', 'must be a non-empty array')
  }
  const [first, ...args1] = args
  if (first && first.prototype instanceof Model) {
    if (args1.length === 1 || args1.length === 2) {
      const key = new Key(first, args1[0])
      return getWithArgs([key, ...args1.slice(1)], callback)
    } else {
      throw new InvalidParameterError('args',
        'Expecting args to have a tuple of (Model, id, optionalOpt).')
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
 * @access package
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
    if (!model.mutated) {
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
      if (this.__toCheck[model] && model.mutated) {
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
        const itemId = Object.values(reason.Item.id)[0]
        const error = new ModelAlreadyExistsError(itemId)
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
   *   get(Cls, id, params)
   *   get(Key, params)
   *   get([Key], params)
   *
   * @param {Model} Cls a Model class.
   * @param {String|CompositeID} id
   * @param {GetParams} [params]
   * @returns Model(s) associated with provided id.
   */
  async get (...args) {
    return getWithArgs(args, async (key, params) => {
      const model = new key.Cls(params)
      model.__checkCompositeID(key.compositeID)
      const getParams = model.__getParams(key.compositeID, params)
      const data = await this.documentClient.get(getParams).promise()
      if ((!params || !params.createIfMissing) && !data.Item) {
        return undefined
      }
      model.__setupModel(data.Item || key.compositeID, !data.Item,
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
    const data = model.__splitIDFromOtherFields(original)[1] // also check ids
    model.__setupModel(original, false, model.constructor.__INIT_METHOD.UPDATE)
    Object.keys(data).forEach(k => {
      model.getField(k).get() // Read to show in ConditionExpression
    })

    Object.keys(updated).forEach(key => {
      if (model.__fields[key].keyType !== undefined) {
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
   * @param {Object} [params] Parameters to be passed to model's constructor
   */
  createOrPut (Cls, original, updated, params) {
    const model = new Cls(params)
    model.__splitIDFromOtherFields(original)
    model.__setupModel(original, true,
      model.constructor.__INIT_METHOD.CREATE_OR_PUT)
    Object.keys(updated).forEach(key => {
      model[key] = updated[key]
    })
    const missingFields = Object.keys(model.__fields).filter(key => {
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
   * @param {GetParams} params Passed into Model's constructor. Options related
   *   to DB are useless.
   */
  create (Cls, data, params) {
    const model = new Cls(params)
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
    const attrs = []
    const keys = []
    const temp = new this()
    const typeMap = {
      String: 'S',
      Number: 'N',
      Object: 'M',
      Array: 'L'
      // Boolean: 'BOOL' // Really shouldn't be hash key
    }
    for (const [name, field] of Object.entries(temp)) {
      if (!(field instanceof __Field)) {
        continue
      }
      if (field.keyType) {
        attrs.push({
          AttributeName: name,
          AttributeType: typeMap[field.valueType.name]
        })
        keys.push({
          AttributeName: name,
          KeyType: field.keyType
        })
      }
    }
    const params = {
      TableName: temp.fullTableName,
      AttributeDefinitions: attrs,
      KeySchema: keys,
      ProvisionedThroughput: {
        ReadCapacityUnits: 2,
        WriteCapacityUnits: 2
      }
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

  const exportAsFactory = [
    ArrayField,
    BooleanField,
    NumberField,
    ObjectField,
    StringField,
    Key
  ]
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
  exportAsFactory.forEach(Cls => {
    toExport[Cls.name] = (...options) => new Cls(...options)
  })

  if (inDebugger) {
    toExport.__private__ = {
      __Field,
      __WriteBatcher,
      getWithArgs
    }
  }
  return toExport
}

module.exports = setup()
