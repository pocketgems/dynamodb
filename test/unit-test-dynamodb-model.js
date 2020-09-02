const S = require('fluent-schema')
const uuidv4 = require('uuid').v4

const { BaseTest, runTests } = require('./base-unit-test')
const db = require('../src/db')

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

async function txGetByKey (key, func) {
  return db.Transaction.run(async tx => {
    const model = await tx.get(key, { createIfMissing: true })
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

  testInvalidIDs () {
    const model = new SimpleModel()
    expect(() => {
      model.__checkCompositeID({})
    }).toThrow(db.InvalidParameterError)

    expect(() => {
      model.__checkCompositeID({ id: 'abc', notAField: 123 })
    }).toThrow(db.InvalidParameterError)
  }

  testInvalidSetup () {
    const model = new SimpleModel()
    expect(() => {
      model.__setupModel({}, true, 'abc')
    }).toThrow(db.InvalidParameterError)
  }

  async testRecreatingTable () {
    // Re-creating the same table shouldn't error out
    await SimpleModel.createUnittestResource()
  }

  async testDebugFunctionExport () {
    // Only export in debugging
    jest.resetModules()
    const oldVal = process.env.INDEBUGGER
    process.env.INDEBUGGER = 0
    const tempDB = require('../src/dynamodb')()
    expect(tempDB.Model.createUnittestResource).toBe(undefined)
    expect(tempDB.Model.__private__).toBe(undefined)
    process.env.INDEBUGGER = oldVal
    jest.resetModules()
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
      { inconsistentRead: false })
    expect(getParams.ConsistentRead).toBe(true)
  }

  async testEventualConsistentGet () {
    const msg = uuidv4()
    const originalFunc = SimpleModel.prototype.__getParams
    const mock = jest.fn().mockImplementation((ignore, params) => {
      expect(params.inconsistentRead).toBe(false)
      // Hard to mock this properly,
      // so just throw with unique msg
      // and make sure it's caught outside
      throw new Error(msg)
    })
    SimpleModel.prototype.__getParams = mock
    const getParams = { inconsistentRead: false, createIfMissing: true }
    const fut = db.Transaction.run(async tx => {
      await tx.get(SimpleModel, uuidv4(), getParams)
    })
    await expect(fut).rejects.toThrow(Error)
    SimpleModel.prototype.__getParams = originalFunc
  }
}

class NewModelTest extends BaseTest {
  async testCreateModelIsNew () {
    const result = await db.Transaction.run(tx => {
      const model = tx.create(SimpleModel, { id: '123' })
      expect(model.id).toBe('123')
      expect(model.id).toBe(SimpleModel.compoundValueToString({ id: '123' }))
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
      .rejects.toThrow(db.ModelAlreadyExistsError)
  }

  async testNewModelParams () {
    const id = uuidv4()
    const params = { b: { d: 321321 } }
    const model = await txCreate(SimpleModel, { id }, params)
    expect(model.id).toBe(id)
    expect(model.params).toStrictEqual(params)
  }
}

class IDWithSchemaModel extends db.Model {}
IDWithSchemaModel.setSchemaForID(
  S.string().pattern(/^xyz.*$/).description(
    'any string that starts with the prefix "xyz"'))

class CompoundIDModel extends db.Model {}
CompoundIDModel.setSchemaForID({
  year: S.integer().minimum(1900),
  make: S.string().minLength(3),
  upc: S.string()
})

class IDSchemaTest extends BaseTest {
  async setUp () {
    await IDWithSchemaModel.createUnittestResource()
    await CompoundIDModel.createUnittestResource()
  }

  async testSimpleIDWithSchema () {
    const cls = IDWithSchemaModel
    const id = 'xyz' + uuidv4()
    const m1 = await txCreate(cls, { id })
    expect(m1.id).toBe(id)
    await expect(txCreate(cls, { id: 'bad' })).rejects.toThrow(
      db.InvalidFieldError)

    // IDs are checked when keys are created too
    expect(() => db.Key(cls, 'bad')).toThrow(db.InvalidFieldError)
    expect(() => cls.compoundValueToString({ id: 'X' })).toThrow(
      db.InvalidFieldError)
    expect(db.Key(cls, 'xyz').compositeID).toEqual({ id: 'xyz' })
    expect(cls.compoundValueToString({ id: 'xyz' })).toEqual('xyz')
  }

  async testCompoundID () {
    const compoundID = { year: 1900, make: 'Honda', upc: uuidv4() }
    const id = CompoundIDModel.compoundValueToString(compoundID)
    function check (entity) {
      expect(entity.id).toBe(id)
      expect(entity.year).toBe(1900)
      expect(entity.make).toBe('Honda')
      expect(entity.upc).toBe(compoundID.upc)
    }

    check(await txCreate(CompoundIDModel, { id }))
    check(await txGet(CompoundIDModel, id))
    check(await txGetByKey(db.Key(CompoundIDModel, id)))
    check(await txGet(CompoundIDModel, compoundID))

    expect(() => db.Key(CompoundIDModel, {})).toThrow(db.InvalidFieldError)
    expect(() => db.Key(CompoundIDModel, {
      year: undefined, // not allowed!
      make: 'Toyota',
      upc: 'nope'
    })).toThrow(db.InvalidFieldError)
    expect(() => db.Key(CompoundIDModel, {
      year: 2020,
      make: 'Toy\0ta', // no null bytes!
      upc: 'nope'
    })).toThrow(db.InvalidFieldError)
    expect(() => db.Key(CompoundIDModel, 'miss')).toThrow(db.InvalidFieldError)
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

  async testPutNoLock () {
    const model = await txGet(BasicModel, this.modelName)
    model.getField('noRequiredNoDefault').incrementBy(1)
    expect(model.__putParams()).not.toHaveProperty(CONDITION_EXPRESSION_STR)
  }

  async testRetry () {
    const model = await txGet(BasicModel, this.modelName)
    const msg = uuidv4()
    const originalFunc = model.documentClient.update
    const mock = jest.fn().mockImplementation((ignore, params) => {
      const err = new Error(msg)
      err.retryable = true
      throw err
    })
    model.documentClient.update = mock
    await expect(model.__write()).rejects.toThrow('Max retries reached')
    expect(mock).toHaveBeenCalledTimes(4)

    model.documentClient.update = originalFunc
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

  async testReadonlyModel () {
    const m1 = await txGet(BasicModel, this.modelName)
    m1.noRequiredNoDefault // eslint-disable-line no-unused-expressions
    expect(m1.__conditionCheckParams()).toHaveProperty('ConditionExpression',
      'attribute_not_exists(noRequiredNoDefault)')
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

  testDeprecatingLegacySyntax () {
    expect(() => {
      db.Key(SimpleModel, 'id', 123)
    }).toThrow()
  }

  testInvalidModelCls () {
    expect(() => {
      db.Key(Object, 'id')
    }).toThrow()
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
      .rejects.toThrow(db.InvalidFieldError)

    await expect(txGet(JSONModel, name, model => {
      model.objNoDefaultRequired = obj
    })).rejects.toThrow(db.InvalidFieldError)

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

class GetArgsParserTest extends BaseTest {
  async testJustAModel () {
    await expect(db.__private__.getWithArgs([SimpleModel], () => {})).rejects
      .toThrow(db.InvalidParameterError)
  }

  async testNoArg () {
    const invalidArgs = [undefined, {}, [], 1, '']
    for (const args of invalidArgs) {
      await expect(db.__private__.getWithArgs(args, () => {})).rejects
        .toThrow(db.InvalidParameterError)
    }
  }

  async testId () {
    const params = [SimpleModel]
    await expect(db.__private__.getWithArgs(params, () => {})).rejects
      .toThrow(db.InvalidParameterError)

    params.push('id')
    expect(async () => {
      const result = await db.__private__.getWithArgs(params, () => 123)
      expect(result).toBe(123)
    }).not.toThrow()

    params.push({})
    expect(async () => {
      const result = await db.__private__.getWithArgs(params, () => 234)
      expect(result).toBe(234)
    }).not.toThrow()

    params[1] = { id: 'id' }
    expect(async () => {
      const result = await db.__private__.getWithArgs(params, () => 23)
      expect(result).toBe(23)
    }).not.toThrow()

    params.push(1)
    await expect(db.__private__.getWithArgs(params, () => {})).rejects
      .toThrow(db.InvalidParameterError)
  }

  async testKey () {
    const params = [SimpleModel.key('id')]
    expect(async () => {
      const result = await db.__private__.getWithArgs(params, () => 123)
      expect(result).toBe(123)
    }).not.toThrow()

    params.push({})
    expect(async () => {
      const result = await db.__private__.getWithArgs(params, () => 234)
      expect(result).toBe(234)
    }).not.toThrow()

    params.push(1)
    await expect(db.__private__.getWithArgs(params, () => {})).rejects
      .toThrow(db.InvalidParameterError)
  }

  async testKeys () {
    const keys = []
    const params = [keys]
    await expect(db.__private__.getWithArgs(params)).rejects
      .toThrow(db.InvalidParameterError)

    keys.push(SimpleModel.key('id'), SimpleModel.key('id1'))
    expect(async () => {
      const result = await db.__private__.getWithArgs(params,
        (key) => key.compositeID)
      expect(result).toStrictEqual([{ id: 'id' }, { id: 'id1' }])
    }).not.toThrow()

    keys.push(1)
    await expect(db.__private__.getWithArgs(params)).rejects
      .toThrow(db.InvalidParameterError)

    keys.splice(2, 1)
    params.push({})
    expect(async () => {
      const result = await db.__private__.getWithArgs(params,
        (key) => key.compositeID)
      expect(result).toStrictEqual([{ id: 'id' }, { id: 'id1' }])
    }).not.toThrow()

    params.push(1)
    await expect(db.__private__.getWithArgs(params)).rejects
      .toThrow(db.InvalidParameterError)
  }
}

class WriteBatcherTest extends BaseTest {
  async setUp () {
    await BasicModel.createUnittestResource()
    this.modelNames = [uuidv4(), uuidv4()]
    const futs = this.modelNames.map(name => {
      return txGet(BasicModel, name, (m) => {
        m.noRequiredNoDefault = 0
      })
    })
    await Promise.all(futs)
  }

  async testUntrackedWrite () {
    const batcher = new db.__private__.__WriteBatcher()
    const model = await txGet(BasicModel, 'id')
    await expect(batcher.__write(model)).rejects.toThrow()
  }

  async testDupWrite () {
    const batcher = new db.__private__.__WriteBatcher()
    const model = await txGet(BasicModel, 'id')
    batcher.track(model)
    model.noRequiredNoDefault += 1
    expect(async () => {
      await batcher.__write(model)
    }).not.toThrow()
    await expect(batcher.__write(model)).rejects.toThrow()
  }

  async testReadonly () {
    const batcher = new db.__private__.__WriteBatcher()
    const model1 = await txGet(BasicModel, this.modelNames[0])
    const model2 = await txGet(BasicModel, this.modelNames[1])
    batcher.track(model1)
    batcher.track(model2)
    model1.noRequiredNoDefault = model2.noRequiredNoDefault + 1
    const originalFunc = batcher.documentClient.transactWrite
    const msg = uuidv4()
    const mock = jest.fn().mockImplementation(data => {
      const update = data.TransactItems[0].Update
      expect(update.ConditionExpression).toBe('noRequiredNoDefault=:_1')
      expect(update.UpdateExpression).toBe('SET noRequiredNoDefault=:_0')
      const condition = data.TransactItems[1].ConditionCheck
      expect(condition.ConditionExpression).toBe('noRequiredNoDefault=:_1')
      throw new Error(msg)
    })
    batcher.documentClient.transactWrite = mock
    await expect(batcher.commit()).rejects.toThrow(msg)
    expect(mock).toHaveBeenCalledTimes(1)

    batcher.documentClient.transactWrite = originalFunc
  }

  testExceptionParser () {
    const reasons = []
    const response = {
      httpResponse: {
        body: {
          toString: function () {
            return JSON.stringify({
              CancellationReasons: reasons
            })
          }
        }
      }
    }

    const batcher = new db.__private__.__WriteBatcher()
    expect(() => {
      batcher.__extractError(response)
    }).not.toThrow()

    reasons.push({
      Code: 'ConditionalCheckFailed',
      Item: { id: '123' }
    })
    expect(() => {
      batcher.__extractError(response)
    }).toThrow(db.ModelAlreadyExistsError)

    reasons[0].Code = 'anything else'
    expect(() => {
      batcher.__extractError(response)
    }).not.toThrow()
  }
}

runTests(
  ErrorTest,
  KeyTest,
  SimpleModelTest,
  JSONModelTest,
  NewModelTest,
  WriteTest,
  ConditionCheckTest,
  IDSchemaTest,
  GetArgsParserTest,
  WriteBatcherTest
)
