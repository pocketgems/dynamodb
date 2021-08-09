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

module.exports = {
  GenericModelError,
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
}
