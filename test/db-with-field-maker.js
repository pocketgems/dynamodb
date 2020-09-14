const assert = require('assert')
const S = require('fluent-schema')

const db = require('../src/dynamodb')

// create helper functions to construct fields for testing purposes
db.__private.fields.forEach(Cls => {
  db.__private[Cls.name] = opts => fieldFromFieldOptions(Cls, opts)
})
function fieldFromFieldOptions (Cls, options) {
  options = options || {}
  let schema
  function processOption (key, func) {
    if (Object.hasOwnProperty.call(options, key)) {
      const val = options[key]
      if (func) {
        schema = func(val)
      }
      delete options[key]
      return val
    }
  }
  // schema is required; fill in the default if none is provided
  processOption('schema', schema => schema)
  if (!schema) {
    if (Cls.name === 'ArrayField') {
      schema = S.array()
    } else if (Cls.name === 'BooleanField') {
      schema = S.boolean()
    } else if (Cls.name === 'NumberField') {
      schema = S.number()
    } else if (Cls.name === 'ObjectField') {
      schema = S.object()
    } else {
      assert.ok(Cls.name === 'StringField', 'unexpected class: ' + Cls.name)
      schema = S.string()
    }
  }
  let initVal
  let valSpecified = true
  if (Object.hasOwnProperty.call(options, 'val')) {
    initVal = options.val
    delete options.val
  } else if (options.default) {
    initVal = undefined
    valSpecified = false
  } else {
    initVal = {
      ArrayField: [],
      BooleanField: false,
      NumberField: 0,
      ObjectField: {},
      StringField: ''
    }[Cls.name]
  }
  const valIsFromDB = processOption('valIsFromDB')
  const keyType = processOption('keyType')
  processOption('optional', isOpt => isOpt ? schema.optional() : schema)
  processOption('immutable', isReadOnly => schema.readOnly(isReadOnly))
  processOption('default', val => schema.default(val))
  const optionKeysLeft = Object.keys(options)
  assert.ok(optionKeysLeft.length === 0,
      `unexpected option(s): ${optionKeysLeft}`)
  const name = 'someName'
  options = db.__private.__Field.__validateFieldOptions(keyType, name, schema)
  return new Cls(name, options, initVal, valIsFromDB, valSpecified, false)
}

module.exports = db
