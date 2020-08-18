const S = require('fluent-schema')

const { BaseTest } = require('./base-unit-test')
const db = require('../src/dynamodb')()

class CommonFieldTest extends BaseTest {
  testFieldSubclassValueType () {
    // Subclasses of __Field must supply a valueType
    const field = new db.__private__.__Field()
    expect(() => {
      field.valueType // eslint-disable-line no-unused-expressions
    }).toThrow()
  }

  makeSureMutableFieldWorks (field) {
    expect(field.validate()).toBe(true)
    field.set(1)
    expect(field.get()).toBe(1)
    expect(field.validate()).toBe(true)
    field.set(2)
    expect(field.get()).toBe(2)
    expect(field.validate()).toBe(true)
  }

  testFieldMutableByDefault () {
    // Make sure fields are mutable by default && they can be mutated
    this.makeSureMutableFieldWorks(db.NumberField({ optional: true }))
  }

  testMutableField () {
    // Make sure explicit mutable field works
    this.makeSureMutableFieldWorks(db.NumberField({ optional: true }))
  }

  testInvalidFieldOption () {
    expect(() => {
      db.NumberField({ aaaa: 1 }) // eslint-disable-line no-new
    }).toThrow(db.InvalidOptionsError)
  }

  testImmutableFieldNoDefault () {
    const field = db.NumberField({ immutable: true, optional: true })
    expect(field.validate()).toBe(true)
    field.set(1)
    expect(field.get()).toBe(1)
    expect(field.validate()).toBe(true)
    expect(() => {
      field.set(2)
    }).toThrow(db.InvalidFieldError)
    expect(() => {
      field.set(undefined)
    }).toThrow(db.InvalidFieldError)
  }

  testImmutableFieldWithDefault () {
    const field = db.NumberField({ immutable: true, default: 1 })
    expect(() => {
      field.set(2)
    }).toThrow(db.InvalidFieldError)
    expect(() => {
      field.set(undefined)
    }).toThrow(db.InvalidFieldError)
  }

  testImmutableFieldWithUndefineDefault () {
    const field = db.NumberField({
      immutable: true,
      default: undefined,
      optional: true
    })
    field.set(2)
    expect(field.get()).toBe(2)
    expect(() => {
      field.set(undefined)
    }).toThrow(db.InvalidFieldError)
  }

  get allFlags () {
    return ['immutable', 'default', 'keyType', 'optional']
  }

  testFlagsExist () {
    const field = db.NumberField()
    this.allFlags.forEach(flag => {
      expect(Object.prototype.hasOwnProperty.call(field, flag)).toBe(true)
    })
  }

  testFlagsImmutable () {
    const field = db.NumberField()
    this.allFlags.forEach(flag => {
      expect(() => {
        field[flag] = 1
      }).toThrow(TypeError)
    })
  }

  testFieldNotExtendable () {
    const field = db.NumberField()
    field.__setup(1)
    expect(() => {
      field.invalidProperty = undefined
    }).toThrow(TypeError)
  }

  testMutatedFlagWithDefault () {
    // Fields with default are mutated by default also
    // so this field will be tranmitted to server on update
    const field = db.NumberField({ default: 1, optional: true })
    expect(field.mutated).toBe(true)

    // Setting field back to undefined makes it not mutated
    field.set(undefined)
    expect(field.mutated).toBe(false)
  }

  testMutatedFlagWithUndefinedDefault () {
    // Undefined values, shouldn't be mutated
    const field = db.NumberField({ default: undefined, optional: true })
    expect(field.mutated).toBe(false)
  }

  testMutatedFlagNoDefault () {
    // Fields with no default aren't mutated after initialization
    let field = db.NumberField({ optional: true })
    expect(field.mutated).toBe(false)

    // Public methods don't affect mutated flag except for `set`
    field.validate()
    expect(field.mutated).toBe(false)

    field.get()
    expect(field.mutated).toBe(false)

    // Setting to undefined should also make the field mutated
    field.set(1)
    expect(field.mutated).toBe(true)

    // Setting up a field makes it mutated
    // So read values from server will not be sent back on update
    field = db.NumberField()
    field.__setup(1)
    expect(field.mutated).toBe(false)
  }

  testMutatedFlagDetectsNestedChanges () {
    // Array and Object fields detects nested mutation correctly
    const deepobj = {}
    const arr = [1, 2, 3, deepobj]
    const arrayField = db.ArrayField()
    arrayField.__setup(arr)
    expect(arrayField.mutated).toBe(false)

    const obj = { key: arr }
    const objectField = db.ObjectField()
    objectField.__setup(obj)
    expect(objectField.mutated).toBe(false)

    deepobj.prop = 1
    expect(arrayField.mutated).toBe(true)
    expect(objectField.mutated).toBe(true)
  }

  testInitialValue () {
    // Initial value is default
    const field = db.NumberField({ default: 1 })
    expect(field.__initialValue).toBe(undefined)

    field.set(2)
    expect(field.__initialValue).toBe(undefined)

    // Initial value is sync'd after setup
    field.__setup(2)
    expect(field.__initialValue).toBe(2)
  }

  testKeyTypeFlag () {
    db.NumberField({ keyType: 'HASH' }) // eslint-disable-line no-new
    db.NumberField({ keyType: 'RANGE' }) // eslint-disable-line no-new
    db.NumberField({ keyType: undefined }) // eslint-disable-line no-new
    expect(() => {
      db.NumberField({ keyType: 1 }) // eslint-disable-line no-new
    }).toThrow(db.InvalidOptionsError)
  }

  testHashKeyImmutable () {
    let field = db.NumberField({ keyType: 'HASH' })
    expect(field.immutable).toBe(true)

    expect(() => {
      db.NumberField({ keyType: 'HASH', immutable: false })
    }).toThrow(db.InvalidOptionsError)

    field = db.NumberField({ keyType: 'HASH', immutable: true })
    expect(field.immutable).toBe(true)

    field = db.NumberField({ keyType: 'RANGE' })
    expect(field.immutable).toBe(true)

    expect(() => {
      db.NumberField({ keyType: 'RANGE', immutable: false })
    }).toThrow(db.InvalidOptionsError)

    field = db.NumberField({ keyType: 'RANGE', immutable: true })
    expect(field.immutable).toBe(true)
  }

  testKeyNoDefault () {
    let field = db.NumberField({ keyType: 'HASH' })
    expect(field.default).toBe(undefined)

    expect(() => {
      db.NumberField({ keyType: 'HASH', default: undefined })
    }).toThrow(db.InvalidOptionsError)

    expect(() => {
      db.NumberField({ keyType: 'HASH', default: 1 })
    }).toThrow(db.InvalidOptionsError)

    field = db.NumberField({ keyType: 'RANGE' })
    expect(field.default).toBe(undefined)

    expect(() => {
      db.NumberField({ keyType: 'RANGE', default: undefined })
    }).toThrow(db.InvalidOptionsError)

    expect(() => {
      db.NumberField({ keyType: 'RANGE', default: 1 })
    }).toThrow(db.InvalidOptionsError)
  }

  testKeyRequired () {
    let field = db.NumberField({ keyType: 'HASH' })
    expect(field.optional).toBe(false)

    field = db.NumberField({ keyType: 'HASH' })
    expect(field.optional).toBe(false)

    expect(() => {
      db.NumberField({ keyType: 'HASH', optional: true })
    }).toThrow(db.InvalidOptionsError)
    field = db.NumberField({ keyType: 'HASH', optional: undefined })
    expect(field.optional).toBe(false)

    expect(() => {
      db.NumberField({ keyType: 'HASH', optional: '' })
    }).toThrow(db.InvalidOptionsError)

    field = db.NumberField({ keyType: 'RANGE' })
    expect(field.optional).toBe(false)

    field = db.NumberField({ keyType: 'RANGE', optional: false })
    expect(field.optional).toBe(false)

    expect(() => {
      db.NumberField({ keyType: 'RANGE', optional: true })
    }).toThrow(db.InvalidOptionsError)
    field = db.NumberField({ keyType: 'RANGE', optional: undefined })
    expect(field.optional).toBe(false)

    expect(() => {
      db.NumberField({ keyType: 'RANGE', optional: '' })
    }).toThrow(db.InvalidOptionsError)
  }

  testRequiredFlag () {
    // Required w/o default
    let field = db.NumberField()
    expect(() => {
      field.validate()
    }).toThrow(db.InvalidFieldError)

    field.set(1)
    expect(field.validate()).toBe(true)

    // Required w/ default
    field = db.NumberField({ default: 1 })
    expect(field.validate()).toBe(true)

    // Required w/ undefined as default. WHY?
    expect(() => {
      const opts = { default: undefined }
      db.NumberField(opts) // eslint-disable-line no-new
    }).toThrow(db.InvalidFieldError)
  }

  testAccessedFlag () {
    let field = db.NumberField({ default: 1, optional: true })
    expect(field.accessed).toBe(false)

    field = db.NumberField({ optional: true })
    expect(field.accessed).toBe(false)

    field.validate()
    expect(field.accessed).toBe(false)

    field.mutated // eslint-disable-line no-unused-expressions
    expect(field.accessed).toBe(false)

    field.__setup(1)
    expect(field.accessed).toBe(false)

    field.get()
    expect(field.accessed).toBe(true)

    field = db.NumberField()
    field.set(1)
    expect(field.accessed).toBe(true)
  }

  testInvalidValueReverts () {
    // When an invalid value is set to a field, an exception will be thrown.
    // But we need to make sure even if somehow the error is caught and ignored
    // the field remains valid.
    const field = db.NumberField({ default: 987 })
    expect(() => {
      field.set('123')
    }).toThrow(db.InvalidFieldError)
    expect(field.get()).toBe(987)
  }
}

class FieldSchemaTest extends BaseTest {
  testInvalidSchemaType () {
    // Make sure mixed schema and field types yields an error at creation time
    expect(() => {
      db.NumberField({ schema: S.object() })
    }).toThrow(db.InvalidOptionsError)
    expect(() => {
      db.ObjectField({ schema: S.array() })
    }).toThrow(db.InvalidOptionsError)
    expect(() => {
      db.ArrayField({ schema: S.object() })
    }).toThrow(db.InvalidOptionsError)
  }

  testValidSchemaType () {
    db.NumberField({ schema: S.number() })
    db.StringField({ schema: S.string() })
    db.ArrayField({ schema: S.array() })
    db.ObjectField({ schema: S.object() })
  }

  testInvalidSchema () {
    // Make sure schema compilation is checked at field initilization time
    expect(() => {
      db.NumberField({ schema: { oaisjdf: 'aosijdf' } })
    }).toThrow()
  }

  testInvalidDefault () {
    // Make sure default values are checked against schema
    expect(() => {
      db.StringField({ schema: S.string().minLength(8), default: '123' })
    }).toThrow(db.InvalidFieldError)
  }

  testStringValidation () {
    // Make sure string schema is checked
    const field = db.StringField({
      schema: S.string().minLength(8).maxLength(9)
    })
    expect(() => {
      field.set('123')
    }).toThrow(db.InvalidFieldError)

    field.set('12345678') // 8 char ok.

    expect(() => {
      field.set('1234567890') // 10 char not ok.
    }).toThrow(db.InvalidFieldError)
  }

  testInvalidObject () {
    // Make sure object schema is checked
    const field = db.ObjectField({
      schema: S.object().prop('abc', S.string().required())
    })

    const invalidValues = [
      {},
      { aaa: 123 },
      { abc: 123 }
    ]
    invalidValues.forEach(val => {
      expect(() => {
        field.set(val)
      }).toThrow(db.InvalidFieldError)
    })
    field.set({ abc: '123' })
  }
}

class RepeatedFieldTest extends BaseTest {
  get fieldFactory () {
    throw new Error('Must be overridden')
  }

  get valueType () {
    throw new Error('Must be overridden')
  }

  get values () {
    return ['a', 1, { a: 1 }, [1], true]
  }

  testInvalidValueType () {
    const field = this.fieldFactory()
    this.values.forEach(val => {
      if (val.constructor.name === this.valueType.name) {
        field.set(val)
        expect(field.get()).toBe(val)
      } else {
        expect(() => {
          field.set(val)
        }).toThrow(db.InvalidFieldError)
      }
    })
  }

  testNoDefaultValue () {
    const field = this.fieldFactory()
    expect(field.get()).toBeUndefined()
  }

  testDefaultValue () {
    this.values.forEach(val => {
      if (val.constructor.name === this.valueType.name) {
        const field = this.fieldFactory({ default: val })
        expect(field.get()).toStrictEqual(val)
      } else {
        expect(() => {
          this.fieldFactory({ default: val }) // eslint-disable-line no-new
        }).toThrow(db.InvalidFieldError)
      }
    })
  }

  testNonExistAttributeConditionValue () {
    // Make sure attribute_not_exist() is generated
    const field = this.fieldFactory()
    field.name = 'myField'
    expect(field.__conditionExpression('')).toStrictEqual(
      ['attribute_not_exists(myField)', {}])
  }
}

class NumberFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.NumberField
  }

  get valueType () {
    return Number
  }

  testRequiredFlagWithFalsyValue () {
    const field = this.fieldFactory({ default: 0 })
    expect(field.validate()).toBe(true)
  }

  testImmutableFlagWithFalsyValue () {
    const field = this.fieldFactory({ immutable: true, default: 0 })
    expect(() => {
      field.set(1)
    }).toThrow(db.InvalidFieldError)
  }

  testIncrementByUndefined () {
    const field = db.NumberField()
    field.incrementBy(123321)
    expect(field.get()).toBe(123321)
  }

  testIncrementByMultipleTimes () {
    const field = db.NumberField()
    field.incrementBy(1)
    field.incrementBy(123)
    field.incrementBy(321)
    expect(field.get()).toBe(445)
  }

  testMixingSetAndIncrementBy () {
    let field = db.NumberField()
    field.incrementBy(1)
    expect(() => {
      field.set(2)
    }).toThrow()

    field = db.NumberField()
    field.set(1)
    expect(() => {
      field.incrementBy(1)
    }).toThrow()
  }

  testDefaultThenIncrementBy () {
    const field = db.NumberField({ default: 1 })
    expect(() => {
      field.incrementBy(1)
    }).not.toThrow()
  }

  testIncrementByNoConditionExpression () {
    const field = db.NumberField()
    field.__setup(0)
    field.incrementBy(1)
    expect(field.accessed).toBe(true)
    expect(field.mutated).toBe(true)
    expect(field.__conditionExpression('')).toStrictEqual([])
  }

  testIncrementByImmutable () {
    const field = db.NumberField({ immutable: true })
    field.incrementBy(1) // Fine
    expect(() => {
      field.incrementBy(2)
    }).toThrow()
  }

  testReadThenIncrementBy () {
    const field = db.NumberField({ immutable: true })
    field.get()
    expect(() => {
      field.incrementBy(788)
    }).not.toThrow()
    expect(field.get()).toBe(788)
  }

  testIncrementByLockInitialUndefined () {
    // Make sure when a field's initial value is undefined (doesn't exists on
    // server), optimistic locking is still performed.
    const field = db.NumberField({ immutable: true })
    field.incrementBy(788)
    expect(field.shouldLock).toBe(true)
  }
}

class StringFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.StringField
  }

  get valueType () {
    return String
  }

  testRequiredFlagWithFalsyValue () {
    const field = this.fieldFactory({ default: '' })
    expect(field.validate()).toBe(true)
  }

  testImmutableFlagWithFalsyValue () {
    const field = this.fieldFactory({ immutable: true, default: '' })
    expect(() => {
      field.set('something')
    }).toThrow(db.InvalidFieldError)
  }
}

class ObjectFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.ObjectField
  }

  get valueType () {
    return Object
  }

  testDefaultDeepCopy () {
    const def = { a: { b: 1 } }
    const field = db.ObjectField({ default: def })
    def.a.b = 2
    expect(field.get().a.b).toBe(1)
  }
}

class BooleanFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.BooleanField
  }

  get valueType () {
    return Boolean
  }

  testRequiredFlagWithFalsyValue () {
    const field = this.fieldFactory({ default: false })
    expect(field.validate()).toBe(true)
  }

  testImmutableFlagWithFalsyValue () {
    const field = this.fieldFactory({ immutable: true, default: false })
    expect(() => {
      field.set(true)
    }).toThrow(db.InvalidFieldError)
  }
}

class ArrayFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.ArrayField
  }

  get valueType () {
    return Array
  }
}

const tests = [
  // Common
  CommonFieldTest,

  // Type specific
  ArrayFieldTest,
  BooleanFieldTest,
  NumberFieldTest,
  ObjectFieldTest,
  StringFieldTest,

  // Other
  FieldSchemaTest
]
tests.forEach(test => test.runTests())
