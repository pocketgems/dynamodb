const uuidv4 = require('uuid').v4

const { BaseTest } = require('./base-unit-test')
const db = require('../src/dynamodb')()

const CONDITION_EXPRESSION_STR = 'ConditionExpression'
const UPDATE_EXPRESSION_STR = 'UpdateExpression'

class ErrorTest extends BaseTest {
  testInvalidFieldError () {
    const err = new db.InvalidFieldError('testField', 'test error')
    expect(err.message).toBe('testField test error')
  }

  testTransactionFailedError () {
    const innerErr = new Error('some error')
    let txErr = new db.TransactionFailedError(innerErr)
    expect(txErr.stack.includes(innerErr.stack))
    expect(txErr.message).toBe('Error: some error')

    txErr = new db.TransactionFailedError('new error')
    expect(txErr.message).toBe('new error')
  }
}

async function txGet (key, id, func) {
  return db.Transaction.run(async tx => {
    const model = await tx.get(key, id, { createIfMissing: true })
    if (func) {
      func(model)
    }
    return model
  })
}

async function txCreate (...args) {
  return db.Transaction.run(tx => {
    return tx.create(...args)
  })
}

class SimpleModel extends db.Model {
  constructor (params) {
    super()
    this.params = params
  }
}

class SimpleModelTest extends BaseTest {
  async setUp () {
    // Create new table should work
    await SimpleModel.createUnittestResource()
  }

  async testRecreatingTable () {
    // Re-creating the same table shouldn't error out
    await SimpleModel.createUnittestResource()
  }

  async testWriteModel () {
    const name = uuidv4()
    await txCreate(SimpleModel, { id: name })
    expect((await txGet(SimpleModel, name)).id).toBe(name)
  }

  async testNoExtension () {
    const model = await txGet(SimpleModel, uuidv4())
    expect(() => {
      model.someprop = 1
    }).toThrow()
  }

  async testIdImmutable () {
    const model = await txGet(SimpleModel, uuidv4())
    expect(() => {
      model.id = 'somethingelse'
    }).toThrow()
  }

  async testEventualConsistentGetParams () {
    const model = new SimpleModel()
    const getParams = model.__getParams(
      { id: '123' },
      { consistentRead: true })
    expect(getParams.ConsistentRead).toBe(true)
  }

  async testEventualConsistentGet () {
    const msg = uuidv4()
    const originalFunc = SimpleModel.prototype.__getParams
    const mock = jest.fn().mockImplementation((ignore, params) => {
      expect(params.consistentRead).toBe(true)
      // Hard to mock this properly,
      // so just throw with unique msg
      // and make sure it's caught outside
      throw new Error(msg)
    })
    SimpleModel.prototype.__getParams = mock
    const getParams = { consistentRead: true, createIfMissing: true }
    const fut = db.Transaction.run(async tx => {
      await tx.get(SimpleModel, uuidv4(), getParams)
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    SimpleModel.prototype.__getParams = originalFunc
  }
}

class NewModelTest extends BaseTest {
  async testCreateModelIsNew () {
    const result = await db.Transaction.run(tx => {
      const model = tx.create(SimpleModel, { id: '123' })
      expect(model.id).toBe('123')
      expect(model.isNew).toBe(true)
      tx.__reset() // Don't write anything, cause it will fail.
      return 321
    })
    expect(result).toBe(321) // Make sure it's done
  }

  async testGetNewModel () {
    let ret = await db.Transaction.run(async tx => {
      return tx.get(SimpleModel, 'something')
    })
    expect(ret).toBe(undefined)

    ret = await db.Transaction.run(async tx => {
      return tx.get(SimpleModel, uuidv4(), { createIfMissing: true })
    })
    expect(ret).not.toBe(undefined)
  }

  async testNewModelWriteCondition () {
    const id = uuidv4()
    await txCreate(SimpleModel, { id })
    await expect(txCreate(SimpleModel, { id }))
      .rejects.toThrow(db.TransactionFailedError)
  }

  async testNewModelParams () {
    const id = uuidv4()
    const params = { b: { d: 321321 } }
    const model = await txCreate(SimpleModel, { id }, params)
    expect(model.id).toBe(id)
    expect(model.params).toStrictEqual(params)
  }
}

class BasicModel extends db.Model {
  constructor (params) {
    super(params)
    this.noRequiredNoDefault = db.NumberField({ optional: true })
  }
}

class WriteTest extends BaseTest {
  async setUp () {
    await BasicModel.createUnittestResource()
    this.modelName = uuidv4()
    await txGet(BasicModel, this.modelName, model => {
      model.noRequiredNoDefault = 0
    })
  }

  async testNoIDInUpdateCondition () {
    const m1 = await txGet(BasicModel, this.modelName)
    const params = m1.__updateParams()
    if (params[CONDITION_EXPRESSION_STR]) {
      expect(params[CONDITION_EXPRESSION_STR]).not.toContain('id=')
    }
  }

  async testNoIdInPutCondition () {
    await txGet(BasicModel, this.modelName, model => {
      const params = model.__putParams()
      if (params.ConditionExpression) {
        expect(params.ConditionExpression).not.toContain('id=')
      }
    })
  }

  async testAttributeEncoding () {
    await txGet(BasicModel, this.modelName, model => {
      model.noRequiredNoDefault += 1
      const params = model.__updateParams()
      expect(params[CONDITION_EXPRESSION_STR]).toContain(
        'noRequiredNoDefault=:_')
      expect(params.ExpressionAttributeValues).toHaveProperty(
        ':_0')
    })
  }

  async testNoAccessProperty () {
    // Building block for strong Transaction isolation levels
    const m1 = await txGet(BasicModel, this.modelName)
    let params = m1.__updateParams()
    expect(params).not.toHaveProperty(CONDITION_EXPRESSION_STR)
    expect(params).not.toHaveProperty(UPDATE_EXPRESSION_STR)

    // Make sure no fields are "accessed" while getting params
    m1.__putParams()
    params = m1.__updateParams()
    expect(params).not.toHaveProperty(CONDITION_EXPRESSION_STR)
    expect(params).not.toHaveProperty(UPDATE_EXPRESSION_STR)
    expect(m1.__fields.id.accessed).toBe(false)
  }

  async testWriteSetToUndefinedProp () {
    // If a field is set to undefined when it's already undefined,
    // the prop should not be transmitted.
    const model = await txGet(BasicModel, uuidv4())
    expect(model.isNew).toBe(true)
    expect(model.noRequiredNoDefault).toBe(undefined)
    model.noRequiredNoDefault = undefined

    const propName = 'noRequiredNoDefault'
    expect(model).toHaveProperty(propName)
    expect(model.__updateParams()).not.toHaveProperty(UPDATE_EXPRESSION_STR)
    expect(model.__putParams().Item).not.toHaveProperty(propName)
  }

  async testResettingProp () {
    // If a field is set to some value then set to undefined again,
    // the change should be handled correctly
    let model = await txGet(BasicModel, uuidv4(), model => {
      expect(model.isNew).toBe(true)
      expect(model.noRequiredNoDefault).toBe(undefined)
      model.noRequiredNoDefault = 1
    })

    // Reset the prop to undefined should delete it
    model = await txGet(BasicModel, model.id, model => {
      expect(model.noRequiredNoDefault).toBe(1)
      model.noRequiredNoDefault = undefined

      const propName = 'noRequiredNoDefault'
      expect(model).toHaveProperty(propName)
      expect(model.__putParams().Item).not.toHaveProperty(propName)
      expect(model.__updateParams()[UPDATE_EXPRESSION_STR])
        .toContain('REMOVE ' + propName)
    })

    // Read and check again
    model = await txGet(BasicModel, model.id)
    expect(model.noRequiredNoDefault).toBe(undefined)
  }

  async testNoLockOption () {
    const model = await txGet(BasicModel, this.modelName)
    model.getField('noRequiredNoDefault').incrementBy(1)
    expect(model.__updateParams()).not.toHaveProperty(CONDITION_EXPRESSION_STR)
  }
}

class ConditionCheckTest extends BaseTest {
  async setUp () {
    await BasicModel.createUnittestResource()
    this.modelName = uuidv4()
    await txGet(BasicModel, this.modelName)
  }

  async testNewModel () {
    const m1 = await txGet(BasicModel, uuidv4())
    expect(m1.isNew).toBe(true)
    expect(m1.mutated).toBe(true)
  }

  async testMutatedModel () {
    const m1 = await txGet(BasicModel, this.modelName)
    expect(m1.mutated).toBe(false)
    m1.noRequiredNoDefault += 1
    expect(m1.mutated).toBe(true)
  }

  async testConditionCheckMutatedModel () {
    const m1 = await txGet(BasicModel, this.modelName)
    m1.noRequiredNoDefault += 1
    expect(() => {
      m1.__conditionCheckParams()
    }).toThrow()
  }

  async testConditionCheckUnchangedModel () {
    const m1 = await txGet(BasicModel, this.modelName)
    expect(m1.__conditionCheckParams()).toStrictEqual(undefined)
  }
}

class RangeKeyModel extends db.Model {
  constructor () {
    super()
    this.rangeKey = db.NumberField({ keyType: 'RANGE' })
  }
}

class KeyTest extends BaseTest {
  async setUp () {
    await Promise.all([
      SimpleModel.createUnittestResource(),
      RangeKeyModel.createUnittestResource()
    ])
    this.modelName = uuidv4()
    const futs = []
    futs.push(txGet(SimpleModel, this.modelName))
    futs.push(txGet(RangeKeyModel, { id: this.modelName, rangeKey: 1 }))
    await Promise.all(futs)
  }

  testValidKey () {
    // These are all correct
    db.Key(SimpleModel, 'id')
    db.Key(SimpleModel, { id: 'id' })
    SimpleModel.key('id')
    SimpleModel.key({ id: 'id' })

    db.Key(RangeKeyModel, { id: 'id', rangeKey: 1 })
    RangeKeyModel.key({ id: 'id', rangeKey: 1 })
  }

  async testInvalidKey () {
    const invalidIDs = [
      1,
      '',
      String(''),
      undefined,
      {},
      []
    ]
    for (const id of invalidIDs) {
      expect(() => {
        db.Key(SimpleModel, id)
      }).toThrow(db.InvalidParameterError)
    }

    // These are technically valid keys, but invalid for the model.
    // Expect no error from instantiating the key, but throw when key
    // if fetched.
    const invalidKeys = [
      db.Key(RangeKeyModel, 'id'),
      db.Key(RangeKeyModel, { id: 'id' }),
      db.Key(RangeKeyModel, { id: 'id', abc: 123 })
    ]

    // Fail here
    const model = new RangeKeyModel()
    for (const key of invalidKeys) {
      expect(() => {
        model.__checkCompositeID(key.compositeID)
      }).toThrow(db.InvalidParameterError)
    }
  }
}

class JSONModel extends db.Model {
  constructor () {
    super()
    this.objNoDefaultNoRequired = db.ObjectField({ optional: true })
    this.objDefaultNoRequired = db.ObjectField({
      default: { a: 1 },
      optional: true
    })
    this.objNoDefaultRequired = db.ObjectField()
    this.objDefaultRequired = db.ObjectField({
      default: {}
    })
    this.arrNoDefaultNoRequired = db.ArrayField({ optional: true })
    this.arrDefaultNoRequired = db.ArrayField({
      default: [1, 2],
      optional: true
    })
    this.arrNoDefaultRequired = db.ArrayField()
    this.arrDefaultRequired = db.ArrayField({
      default: []
    })
  }
}

class JSONModelTest extends BaseTest {
  async setUp () {
    await JSONModel.createUnittestResource()
  }

  async testRequiredFields () {
    const obj = { ab: 2 }
    const arr = [2, 1]
    const name = uuidv4()
    await expect(txGet(JSONModel, name))
      .rejects.toThrow(db.TransactionFailedError)

    await expect(txGet(JSONModel, name, model => {
      model.objNoDefaultRequired = obj
    })).rejects.toThrow(db.TransactionFailedError)

    await txGet(JSONModel, name, model => {
      model.objNoDefaultRequired = obj
      model.arrNoDefaultRequired = arr
    })

    const model = await txGet(JSONModel, name)
    expect(model.arrNoDefaultRequired).toStrictEqual(arr)
    expect(model.objNoDefaultRequired).toStrictEqual(obj)
  }

  async testDeepUpdate () {
    const obj = { ab: [] }
    const arr = [{}]
    const name = uuidv4()
    await txGet(JSONModel, name, model => {
      expect(model.isNew).toBe(true)
      model.objNoDefaultRequired = obj
      model.arrNoDefaultRequired = arr
    })

    await txGet(JSONModel, name, model => {
      obj.ab.push(1)
      model.objNoDefaultRequired.ab.push(1)
      arr[0].bc = 32
      model.arrNoDefaultRequired[0].bc = 32
    })

    await txGet(JSONModel, name, model => {
      expect(model.objNoDefaultRequired).toStrictEqual(obj)
      expect(model.arrNoDefaultRequired).toStrictEqual(arr)
    })
  }
}

const tests = [
  ErrorTest,
  KeyTest,
  SimpleModelTest,
  JSONModelTest,
  NewModelTest,
  WriteTest,
  ConditionCheckTest
]
tests.forEach(test => test.runTests())
