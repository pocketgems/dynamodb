const S = require('@pocketgems/schema')
const { BaseTest, runTests } = require('@pocketgems/unit-test')

const { InvalidFieldError, NotImplementedError } = require('../src/errors')
const { __CompoundField, __FieldInterface } = require('../src/fields')

const db = require('./db-with-field-maker')

class CommonFieldTest extends BaseTest {
  makeSureMutableFieldWorks (field) {
    field.validate()
    field.set(1)
    expect(field.get()).toBe(1)
    field.validate()
    field.set(2)
    expect(field.get()).toBe(2)
    field.validate()
  }

  testFieldMutableByDefault () {
    // Make sure fields are mutable by default && they can be mutated
    this.makeSureMutableFieldWorks(db.__private.NumberField({ optional: true }))
  }

  testMutableField () {
    // Make sure explicit mutable field works
    this.makeSureMutableFieldWorks(db.__private.NumberField({ optional: true }))
  }

  testInvalidFieldOption () {
    expect(() => {
      db.__private.NumberField({ aaaa: 1 }) // eslint-disable-line no-new
    }).toThrow(/unexpected option/)
  }

  testInvalidFieldName () {
    expect(() => {
      db.__private.__Field.__validateFieldOptions(
        'FakeModelName', undefined, '_nope', S.str)
    }).toThrow(/may not start with/)
  }

  testImmutableFieldNoDefault () {
    const field = db.__private.NumberField({
      immutable: true,
      optional: true,
      val: 1
    })
    field.validate()
    expect(field.get()).toBe(1)
    expect(() => {
      field.set(2)
    }).toThrow(db.InvalidFieldError)
    expect(() => {
      field.set(undefined)
    }).toThrow(db.InvalidFieldError)
  }

  testImmutableFieldWithDefault () {
    const field = db.__private.NumberField({ immutable: true, default: 1 })
    expect(() => {
      field.set(2)
    }).toThrow(db.InvalidFieldError)
    expect(() => {
      field.set(undefined)
    }).toThrow(db.InvalidFieldError)
  }

  testImmutableFieldWithUndefinedDefault () {
    const field = db.__private.NumberField({
      immutable: true,
      optional: true,
      val: 2
    })
    expect(field.get()).toBe(2)
    // cannot remove it once set, even though it's optional
    expect(() => {
      field.set(undefined)
    }).toThrow(db.InvalidFieldError)
  }

  get allFlags () {
    return ['immutable', 'default', 'keyType', 'optional']
  }

  testFlagsExist () {
    const field = db.__private.NumberField()
    this.allFlags.forEach(flag => {
      expect(Object.prototype.hasOwnProperty.call(field, flag)).toBe(true)
    })
  }

  testFlagsImmutable () {
    const field = db.__private.NumberField()
    this.allFlags.forEach(flag => {
      expect(() => {
        field[flag] = 1
      }).toThrow(TypeError)
    })
  }

  testMutatedFlagWithDefault () {
    // Fields with default are mutated by default also
    // so this field will be tranmitted to server on update
    const field = db.__private.NumberField({ default: 1, optional: true })
    expect(field.mutated).toBe(true)
    // The change should be committed in a read-write tx
    expect(field.hasChangesToCommit(true)).toBe(true)
    expect(field.hasChangesToCommit()).toBe(true)
    // The change should NOT be committed in a read-only tx
    expect(field.hasChangesToCommit(false)).toBe(false)

    // Setting field back to undefined makes it not mutated
    field.set(undefined)
    expect(field.mutated).toBe(false)
  }

  testMutatedFlagWithUndefinedDefault () {
    // Undefined values, shouldn't be mutated
    const field = db.__private.NumberField({ optional: true, val: undefined })
    expect(field.mutated).toBe(false)
  }

  testMutatedFlagNoDefault () {
    // Fields with no default aren't mutated after initialization
    let field = db.__private.NumberField({ optional: true, val: undefined })
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
    field = db.__private.NumberField({ valIsFromDB: true, val: 1 })
    expect(field.mutated).toBe(false)
  }

  testMutatedFlagDetectsNestedChanges () {
    // Array and Object fields detects nested mutation correctly
    const deepobj = {}
    const arr = [{}, {}, deepobj]
    const arrSchema = S.arr(S.obj({ prop: S.int.optional() }))
    const arrayField = db.__private.ArrayField({
      valIsFromDB: true,
      val: arr,
      schema: arrSchema
    })
    expect(arrayField.mutated).toBe(false)

    const obj = { key: arr }
    const objectField = db.__private.ObjectField({
      valIsFromDB: true,
      val: obj,
      schema: S.obj({ key: arrSchema.copy().optional() })
    })
    expect(objectField.mutated).toBe(false)

    deepobj.prop = 1
    // Expected, since val from database should not change, so __mayHaveMutated
    // flag short-circuits the check
    expect(arrayField.mutated).toBe(false)
    expect(objectField.mutated).toBe(false)

    // Simply reading the field triggers mutated value change
    arrayField.get()
    objectField.get()
    expect(arrayField.mutated).toBe(true)
    expect(objectField.mutated).toBe(true)
  }

  testInitialValue () {
    // Initial value is default
    const field = db.__private.NumberField({ default: 1 })
    expect(field.__initialValue).toBe(undefined)

    field.set(2)
    expect(field.__initialValue).toBe(undefined)

    // Initial value is sync'd after setup
    const field2 = db.__private.NumberField({ valIsFromDB: true, val: 2 })
    expect(field2.__initialValue).toBe(2)
  }

  testHashKeyImmutable () {
    let field = db.__private.NumberField({ keyType: 'PARTITION' })
    expect(field.immutable).toBe(true)

    expect(() => {
      db.__private.NumberField({ keyType: 'PARTITION', immutable: false })
    }).toThrow(db.InvalidOptionsError)

    field = db.__private.NumberField({ keyType: 'PARTITION', immutable: true })
    expect(field.immutable).toBe(true)

    field = db.__private.NumberField({ keyType: 'SORT' })
    expect(field.immutable).toBe(true)

    expect(() => {
      db.__private.NumberField({ keyType: 'SORT', immutable: false })
    }).toThrow(db.InvalidOptionsError)

    field = db.__private.NumberField({ keyType: 'SORT', immutable: true })
    expect(field.immutable).toBe(true)
  }

  testKeyNoDefault () {
    let field = db.__private.NumberField({ keyType: 'PARTITION' })
    expect(field.default).toBe(undefined)

    expect(() => {
      db.__private.NumberField({ keyType: 'PARTITION', default: 1 })
    }).toThrow(db.InvalidOptionsError)

    field = db.__private.NumberField({ keyType: 'SORT' })
    expect(field.default).toBe(undefined)

    // sort keys can have defaults
    field = db.__private.NumberField({ keyType: 'SORT', default: 1 })
    expect(field.default).toBe(1)
  }

  testKeyRequired () {
    let field = db.__private.NumberField({ keyType: 'PARTITION' })
    expect(field.optional).toBe(false)

    field = db.__private.NumberField({ keyType: 'PARTITION' })
    expect(field.optional).toBe(false)

    expect(() => {
      db.__private.NumberField({ keyType: 'PARTITION', optional: true })
    }).toThrow(db.InvalidOptionsError)
    field = db.__private.NumberField({
      keyType: 'PARTITION',
      optional: undefined
    })
    expect(field.optional).toBe(false)

    field = db.__private.NumberField({ keyType: 'SORT' })
    expect(field.optional).toBe(false)

    field = db.__private.NumberField({ keyType: 'SORT', optional: false })
    expect(field.optional).toBe(false)

    expect(() => {
      db.__private.NumberField({ keyType: 'SORT', optional: true })
    }).toThrow(db.InvalidOptionsError)
    field = db.__private.NumberField({ keyType: 'SORT', optional: undefined })
    expect(field.optional).toBe(false)
  }

  testRequiredFlag () {
    // Required w/o default
    expect(() => {
      db.__private.NumberField({ val: undefined })
    }).toThrow(db.InvalidFieldError)

    let field = db.__private.NumberField({ val: 0 })
    field.set(1)
    field.validate()

    // Required w/ default
    field = db.__private.NumberField({ default: 1 })
    field.validate()

    // Required w/ undefined as default. There's no point since the default is
    // already undefined. So we disallow this so there's only one right way
    // for the default value to be undefined.
    expect(() => {
      const opts = { default: undefined }
      db.__private.NumberField(opts) // eslint-disable-line no-new
    }).toThrow('Default value must be defined')
  }

  testAccessedFlag () {
    let field = db.__private.NumberField({ default: 1, optional: true })
    expect(field.accessed).toBe(false)

    field = db.__private.NumberField({ optional: true })
    expect(field.accessed).toBe(false)

    field.validate()
    expect(field.accessed).toBe(false)

    field.mutated // eslint-disable-line no-unused-expressions
    expect(field.accessed).toBe(false)

    field = db.__private.NumberField({ valIsFromDB: true, val: 1 })
    expect(field.accessed).toBe(false)

    field.get()
    expect(field.accessed).toBe(true)

    field = db.__private.NumberField()
    field.set(1)
    expect(field.accessed).toBe(true)
  }

  testInvalidValueReverts () {
    // When an invalid value is set to a field, an exception will be thrown.
    // But we need to make sure even if somehow the error is caught and ignored
    // the field remains valid.
    const field = db.__private.NumberField({ default: 987 })
    expect(() => {
      field.set('123')
    }).toThrow(S.ValidationError)
    expect(field.get()).toBe(987)
  }
}

class FieldSchemaTest extends BaseTest {
  testValidSchemaType () {
    db.__private.NumberField({ schema: S.double })
    db.__private.StringField({ schema: S.str })
    db.__private.ArrayField({ schema: S.arr() })
    db.__private.ObjectField({ schema: S.obj() })
  }

  testInvalidSchema () {
    // Make sure schema compilation is checked at field initilization time
    expect(() => {
      db.__private.NumberField({ schema: { oaisjdf: 'aosijdf' } })
    }).toThrow()
  }

  testInvalidDefault () {
    // Make sure default values are checked against schema
    expect(() => {
      db.__private.StringField({ schema: S.str.min(8), default: '123' })
    }).toThrow(S.ValidationError)
  }

  testStringValidation () {
    // Make sure string schema is checked
    const field = db.__private.StringField({
      val: '123456789',
      schema: S.str.min(8).max(9)
    })
    expect(() => {
      field.set('123')
    }).toThrow(S.ValidationError)

    field.set('12345678') // 8 char ok.

    expect(() => {
      field.set('1234567890') // 10 char not ok.
    }).toThrow(S.ValidationError)
  }

  testInvalidObject () {
    // Make sure object schema is checked
    const field = db.__private.ObjectField({
      val: { abc: 'x' },
      schema: S.obj().prop('abc', S.str)
    })

    const invalidValues = [
      {},
      { aaa: 123 },
      { abc: 123 }
    ]
    invalidValues.forEach(val => {
      expect(() => {
        field.set(val)
      }).toThrow(S.ValidationError)
    })
    field.set({ abc: '123' })
  }
}

class RepeatedFieldTest extends BaseTest {
  get fieldFactory () {
    throw new Error('Must be overridden')
  }

  get someGoodValue () {
    throw new Error('Must be overridden')
  }

  get valueType () {
    throw new Error('Must be overridden')
  }

  get values () {
    return ['a', 1, { a: 1 }, [1], true]
  }

  get valueSchema () {
    return undefined
  }

  testInvalidValueType () {
    const field = this.fieldFactory({ schema: this.valueSchema })
    this.values.forEach(val => {
      if (val.constructor.name === this.valueType.name) {
        field.set(val)
        expect(field.get()).toBe(val)
      } else {
        expect(() => {
          field.set(val)
        }).toThrow(S.ValidationError)
      }
    })
  }

  testNoDefaultValue () {
    const field = this.fieldFactory(({ optional: true, val: undefined }))
    expect(field.get()).toBeUndefined()
  }

  testDefaultValue () {
    this.values.forEach(val => {
      if (val.constructor.name === this.valueType.name) {
        const field = this.fieldFactory({
          default: val,
          schema: this.valueSchema
        })
        expect(field.get()).toStrictEqual(val)
      } else {
        expect(() => {
          this.fieldFactory({ default: val }) // eslint-disable-line no-new
        }).toThrow(S.ValidationError)
      }
    })
  }

  testNonExistAttributeConditionValue () {
    // Make sure attribute_not_exist() is generated for keys even if they are
    // not read
    let field = this.fieldFactory({ keyType: 'PARTITION' })
    field.name = 'myField'
    expect(field.__conditionExpression('')).toStrictEqual(
      [`attribute_not_exists(${field.__awsName})`, {}])

    // non-key and unread means no condition
    field = this.fieldFactory()
    field.name = 'myField'
    expect(field.__conditionExpression('')).toEqual([])

    // once it is read, it will generate a condition though
    field.get()
    expect(field.__conditionExpression('')).toStrictEqual(
      [`attribute_not_exists(${field.__awsName})`, {}])
  }
}

class NumberFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.__private.NumberField
  }

  get someGoodValue () { return 0 }

  get valueType () {
    return Number
  }

  testRequiredFlagWithFalsyValue () {
    this.fieldFactory({ default: 0 }).validate()
  }

  testImmutableFlagWithFalsyValue () {
    const field = this.fieldFactory({ immutable: true, default: 0 })
    expect(() => {
      field.set(1)
    }).toThrow(db.InvalidFieldError)
  }

  testIncrementByUndefined () {
    const field = db.__private.NumberField()
    field.incrementBy(123321)
    expect(field.get()).toBe(123321)
  }

  testIncrementByMultipleTimes () {
    function check (isNew) {
      const opts = isNew ? {} : { valIsFromDB: true, val: 0 }
      const field = db.__private.NumberField(opts)
      let expValue = 0
      const nums = [1, 123, 321]
      for (let i = 0; i < nums.length; i++) {
        const n = nums[i]
        expValue += n
        field.incrementBy(n)
      }
      expect(field.canUpdateWithoutCondition).toBe(true)
      expect(field.canUpdateWithIncrement).toBe(!isNew)
      expect(field.get()).toBe(expValue)
    }
    check(true)
    check(false)
  }

  testMixingSetAndIncrementBy () {
    let field = db.__private.NumberField()
    field.incrementBy(1)
    expect(() => {
      field.set(2)
    }).not.toThrow()
    expect(field.canUpdateWithoutCondition).toBe(true)
    expect(field.canUpdateWithIncrement).toBe(false)
    expect(field.get()).toBe(2)
    // we set the field without ever reading it, so we aren't conditioned on
    // its value
    expect(field.canUpdateWithoutCondition).toBe(true)

    field = db.__private.NumberField()
    field.set(1)
    expect(() => {
      field.incrementBy(1)
    }).not.toThrow()
    expect(field.canUpdateWithoutCondition).toBe(true)
    expect(field.canUpdateWithIncrement).toBe(false)
    expect(field.get()).toBe(2)
    expect(field.canUpdateWithoutCondition).toBe(true)
  }

  testDefaultThenIncrementBy () {
    const field = db.__private.NumberField({ default: 1 })
    expect(() => {
      field.incrementBy(1)
    }).not.toThrow()
  }

  testIncrementByNoConditionExpression () {
    const field = db.__private.NumberField({ valIsFromDB: true, val: 0 })
    field.incrementBy(1)
    expect(field.accessed).toBe(true)
    expect(field.mutated).toBe(true)
    expect(field.__conditionExpression('')).toStrictEqual([])
  }

  testIncrementByImmutable () {
    const field = db.__private.NumberField({ immutable: true, val: 1 })
    expect(() => {
      field.incrementBy(2)
    }).toThrow()
  }

  testReadThenIncrementBy () {
    const field = db.__private.NumberField({ val: 0 })
    field.get()
    expect(() => {
      field.incrementBy(788)
    }).not.toThrow()
    expect(field.get()).toBe(788)
  }

  testIncrementByLockInitialUndefined () {
    // Make sure when a field's initial value is undefined (doesn't exists on
    // server), optimistic locking is still performed.
    const field = db.__private.NumberField({ optional: true, val: undefined })
    expect(() => field.incrementBy(788)).toThrow(
      'cannot increment a field whose value is undefined')
  }
}

class StringFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.__private.StringField
  }

  get someGoodValue () { return 'ok' }

  get valueType () {
    return String
  }

  testRequiredFlagWithFalsyValue () {
    this.fieldFactory({ default: '' }).validate()
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
    return db.__private.ObjectField
  }

  get someGoodValue () {
    return { a: 123 }
  }

  get valueType () {
    return Object
  }

  get valueSchema () {
    return S.obj({ a: S.int.optional() })
  }

  testDefaultDeepCopy () {
    const def = { a: { b: 1 } }
    const field = db.__private.ObjectField({
      default: def,
      schema: S.obj({
        a: S.obj({
          b: S.int
        })
      })
    })
    def.a.b = 2
    expect(field.get().a.b).toBe(1)
  }
}

class BooleanFieldTest extends RepeatedFieldTest {
  get fieldFactory () {
    return db.__private.BooleanField
  }

  get someGoodValue () { return true }

  get valueType () {
    return Boolean
  }

  testRequiredFlagWithFalsyValue () {
    this.fieldFactory({ default: false }).validate()
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
    return db.__private.ArrayField
  }

  get someGoodValue () { return [] }

  get valueType () {
    return Array
  }
}

class CompoundFieldTest extends BaseTest {
  async beforeEach () {
    this.__numField = db.__private.NumberField({
      val: 10,
      optional: true,
      valIsFromDB: true
    })
    this.__numField.name = 'num'
    this.__strField = db.__private.StringField({ val: 'test', optional: true })
    this.__strField.name = 'str'
    this.__objField = db.__private.ObjectField({
      default: { a: { b: 1 } },
      schema: S.obj({ a: S.obj({ b: S.int }) })
    })
    this.__objField.name = 'obj'
  }

  async testInvalidFieldTypes () {
    expect(() => {
      // eslint-disable-next-line no-new
      new __CompoundField({ idx: '1', fields: [this.__numField, 'str'] })
    }).toThrow(InvalidFieldError)
  }

  async testValueEncoding () {
    const field = new __CompoundField({ idx: '1', fields: [this.__numField, this.__strField, this.__objField] })
    expect(field.get()).toBe(['10', '{"a":{"b":1}}', 'test'].join('\0'))
    this.__numField.incrementBy(5)
    this.__objField.__value.a.b = 4
    expect(field.get()).toBe(['15', '{"a":{"b":4}}', 'test'].join('\0'))
    this.__strField.set(undefined)
    expect(field.get()).toBe(undefined)
    this.__strField.set('abc' + '\0' + 'xyz')
    expect(
      () => field.get()
    ).toThrow(/str cannot put null bytes in strings in compound values/)

    const field2 = new __CompoundField({ fields: [this.__numField] })
    expect(field2.get()).toBe(this.__numField.__value)
  }

  async testValueDecoding () {
    function validateDecoding (data) {
      const fields = Object.keys(data)
      const encodedName = __CompoundField.__encodeName(fields)
      const encodedVal = __CompoundField.__encodeValues(fields, data)
      expect(__CompoundField.__decodeValues(encodedName, encodedVal)).toEqual(data)
    }

    expect(__CompoundField.__decodeValues('randomName', '1')).toEqual({})
    expect(() => __CompoundField.__decodeValues('_c_a', [1, 2].join('\0'))
    ).toThrow(/Trying to decode compound field value with unequal amount of properties/)

    validateDecoding({ intField: 1 })
    validateDecoding({ strField: 'abc' })
    validateDecoding({ boolField: true })
    validateDecoding({ arrField: [1, 'a'] })
    validateDecoding({ objField: { a: 1, b: 2 } })
    validateDecoding({ intField: 1, arrField: [10, 'a'] })
  }

  async testValidate () {
    const field = new __CompoundField({ fields: [this.__numField, this.__strField, this.__objField] })
    expect(field.validate()).toBe(true)
    // Compound field returns true always. This is because of the assumption
    // that the underlying fields are validated already and we don't want to add redundant validations.
    this.__numField.__value = 'a'
    expect(field.validate()).toBe(true)
  }

  async testAccessed () {
    const field1 = new __CompoundField({ fields: [this.__numField] })
    const field2 = new __CompoundField({ fields: [this.__numField, this.__strField, this.__objField] })

    expect(field1.accessed).toBe(false)
    expect(field2.accessed).toBe(false)

    this.__strField.get()
    expect(field2.accessed).toBe(true)

    this.__numField.get()
    expect(field1.accessed).toBe(true)
    expect(field2.accessed).toBe(true)
  }

  async testMutated () {
    const field1 = new __CompoundField({ isNew: false, fields: [this.__numField] })
    const field2 = new __CompoundField({ isNew: true, fields: [this.__numField] })
    const field3 = new __CompoundField({ isNew: true, fields: [this.__numField, this.__strField] })

    expect(field1.mutated).toBe(false)
    expect(field2.mutated).toBe(true)
    expect(field3.mutated).toBe(true)

    this.__numField.__value = undefined
    expect(field1.__mayHaveMutated).toBe(false)
    expect(field2.__mayHaveMutated).toBe(true)

    expect(field1.mutated).toBe(false)
    expect(field2.mutated).toBe(true)
    expect(field3.mutated).toBe(true)

    this.__strField.__value = undefined
    expect(field3.mutated).toBe(true)
  }

  async testSetValue () {
    const field = new __CompoundField({ name: 'dummy', fields: [this.__numField] })
    expect(() => {
      field.set(10)
    }).toThrow(InvalidFieldError)
  }

  async testUpdateExpression () {
    const field = new __CompoundField({ idx: '1', isNew: false, fields: [this.__numField] })
    expect(field.__updateExpression('1')).toEqual([])

    const exprKey = '_1'
    const field2 = new __CompoundField({ idx: '1', isNew: true, fields: [this.__numField] })
    expect(field2.__updateExpression(exprKey)).toEqual(
      ['#1=_1', { [exprKey]: field2.__value }, false])

    const field3 = new __CompoundField({ idx: '1', isNew: false, fields: [this.__numField, this.__strField] })
    this.__numField.incrementBy(10)
    const [set, vals, remove] = field3.__updateExpression(exprKey)
    expect(set).toBe('#1=_1')
    expect(vals).toEqual({ [exprKey]: field3.__value })
    expect(remove).toBe(false)

    this.__strField.__value = undefined
    expect(field3.__updateExpression('1')).toEqual([undefined, {}, true])

    this.__numField.__value = undefined
    expect(field2.__updateExpression('1')).toEqual([])

    expect(field.canUpdateWithoutCondition).toBe(true)
    expect(field.__conditionExpression('')).toEqual([])
  }
}

class AbstractFieldTest extends BaseTest {
  testCreatingAbstractField () {
    // eslint-disable-next-line no-new
    expect(() => { new __FieldInterface() }).toThrow(Error)
  }

  testAbstractMethods () {
    class DummyCls extends __FieldInterface {}
    const obj = new DummyCls()
    expect(() => obj.__awsName).toThrow(NotImplementedError)
    expect(() => obj.mutated).toThrow(NotImplementedError)
    expect(() => obj.__mayHaveMutated).toThrow(NotImplementedError)
    expect(() => obj.accessed).toThrow(NotImplementedError)
    expect(() => obj.get()).toThrow(NotImplementedError)
    expect(() => obj.set('')).toThrow(NotImplementedError)
    expect(() => obj.__updateExpression('')).toThrow(NotImplementedError)
    expect(() => obj.__conditionExpression('')).toThrow(NotImplementedError)
    expect(() => obj.validate()).toThrow(NotImplementedError)
    expect(() => obj.hasChangesToCommit()).toThrow(NotImplementedError)
  }
}

runTests(
  // Common
  CommonFieldTest,

  // Type specific
  ArrayFieldTest,
  BooleanFieldTest,
  NumberFieldTest,
  ObjectFieldTest,
  StringFieldTest,
  CompoundFieldTest,

  // Other
  FieldSchemaTest,
  AbstractFieldTest
)
