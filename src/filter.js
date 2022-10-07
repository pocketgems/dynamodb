const {
  InvalidParameterError, InvalidFilterError
} = require('./errors')

/**
 * A filter handle for model fields.
 * Used to construct filter expressions for iterating DB using query or scan.
 */
class Filter {
  /**
   * Create a filter for field
   * @param {String} iterationMethod The iteration method, one of query or scan
   * @param {String} fieldName The field's name.
   * @param {String} awsName The aws name for the field
   */
  constructor (iterationMethod, fieldName, awsName, keyType) {
    if (arguments.length !== 4) {
      throw new InvalidParameterError('Filter expects 4 constructor inputs')
    }
    if (!['query', 'scan'].includes(iterationMethod)) {
      throw new InvalidParameterError(
        'Filter must be created for query or scan')
    }
    if (!['PARTITION', 'SORT', undefined].includes(keyType)) {
      throw new InvalidParameterError(
        'keyType must be one of PARTITION, SORT and undefined')
    }
    this.__method = iterationMethod
    this.__fieldName = fieldName
    this.__awsName = awsName
    this.__keyType = keyType
    this.__locked = false
  }

  static VALID_OPERATIONS = new Set([
    '==', '!=', '<', '<=', '>', '>=', 'between', 'prefix', 'contains'
  ])

  /**
   * Impose a condition on the model field
   * @param {String} operation See VALID_OPERATIONS
   * @param {*} value The RHS of the condition
   */
  filter (operation, value) {
    if (this.__locked) {
      throw new InvalidFilterError(
        'Filter can no longer be changed')
    }
    if (this.__operation) {
      throw new InvalidFilterError(
        `Filter on field ${this.__fieldName} already exists`)
    }
    if (!this.constructor.VALID_OPERATIONS.has(operation)) {
      throw new InvalidFilterError(
        'Invalid filter operation. Valid operations are ' +
        this.constructor.VALID_OPERATIONS)
    }
    if (operation !== '==' && this.__keyType === 'PARTITION') {
      throw new InvalidFilterError(
        'Only equality filters are allowed on partition keys')
    }
    if (operation === '!=' && this.__keyType !== undefined) {
      throw new InvalidFilterError(
        'Inequality filters are not allowed on keys')
    }
    if (operation === 'prefix') {
      if (this.__keyType !== 'SORT') {
        throw new InvalidFilterError(
          'Prefix filters are only allowed on sort keys')
      }
      if (this.__method === 'scan') {
        throw new InvalidFilterError(
          'Invalid "prefix" operation for scan.')
      }
    }
    if (operation === 'between') {
      if (arguments.length !== 3) {
        throw new InvalidFilterError(
          `"between" operator requires 2 value inputs, e.g.
           query.${this.__fieldName}('between', lower, upper)`)
      }
      value = [arguments[1], arguments[2]]
      if (value[0] > value[1]) {
        throw new InvalidFilterError(
          'Input values for "between" operator must be in ascending order')
      }
    }
    if (operation === 'contains' && this.__keyType !== undefined) {
      throw new InvalidFilterError(
        '"contains" filters are not allowed on keys')
    }

    this.__operation = operation
    this.__value = value
    this.__createFilterExpression(operation, value)
  }

  /**
   * Lock the filter to prevent further modifications
   */
  lock () {
    this.__locked = true
  }

  __createFilterExpression (operation, value) {
    const awsName = this.__awsName
    if (operation === 'between') {
      this.conditions = [`#_${awsName} BETWEEN :_${awsName}Lower AND ` +
        `:_${awsName}Upper`]
      this.attrNames = { [`#_${awsName}`]: this.__fieldName }
      this.attrValues = {
        [`:_${awsName}Lower`]: value[0],
        [`:_${awsName}Upper`]: value[1]
      }
      return
    }
    if (operation === 'prefix') {
      this.conditions = [`begins_with(#_${awsName},:_${awsName})`]
      this.attrNames = { [`#_${awsName}`]: this.__fieldName }
      this.attrValues = { [`:_${awsName}`]: value }
      return
    }
    if (operation === 'contains') {
      this.conditions = [`contains(#_${awsName},:_${awsName})`]
      this.attrNames = { [`#_${awsName}`]: this.__fieldName }
      this.attrValues = { [`:_${awsName}`]: value }
      return
    }

    const operator = this.awsOperator
    this.conditions = [`#_${awsName}${operator}:_${awsName}`]
    this.attrNames = { [`#_${awsName}`]: this.__fieldName }
    this.attrValues = { [`:_${awsName}`]: value }
  }

  get awsOperator () {
    return {
      '==': '=',
      '!=': '<>',
      '<': '<',
      '<=': '<=',
      '>': '>',
      '>=': '>=',
      prefix: 'prefix',
      between: 'between',
      contains: 'contains'
    }[this.__operation]
  }
}

module.exports = Filter
