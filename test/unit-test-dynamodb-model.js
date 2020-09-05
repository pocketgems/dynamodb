const S = require('fluent-schema')
const uuidv4 = require('uuid').v4

const { BaseTest, runTests } = require('./base-unit-test')
const db = require('../src/dynamodb')

const CONDITION_EXPRESSION_STR = 'ConditionExpression'
const UPDATE_EXPRESSION_STR = 'UpdateExpression'

class BadModelTest extends BaseTest {
  check (cls, msg) {
    expect(() => cls.__doOneTimeModelPrep()).toThrow(msg)
  }

  testMissingPrimaryKey () {
    class BadModel extends db.Model {
      static KEY = {}
    }
    this.check(BadModel, /at least one partition key field/)
    class BadModel2 extends db.Model {
      static KEY = null
    }
    this.check(BadModel2, /partition key is required/)
  }

  testDuplicateField () {
    const expMsg = /more than once/
    class BadModel extends db.Model {
      static KEY = { name: S.string() }
      static FIELDS = { name: S.string() }
    }
    this.check(BadModel, expMsg)

    class BadModel2 extends db.Model {
      static SORT_KEY = { name: S.string() }
      static FIELDS = { name: S.string() }
    }
    this.check(BadModel2, expMsg)
  }

  testReservedName () {
    class BadModel extends db.Model {
      static SORT_KEY = { isNew: S.string() }
    }
    this.check(BadModel, /this name is reserved/)
  }

  testIDName () {
    class IDCannotBePartOfACompoundPartitionKey extends db.Model {
      static KEY = { id: S.string(), n: S.number() }
    }
    this.check(IDCannotBePartOfACompoundPartitionKey, /lone partition key/)

    class IDMustBeAString extends db.Model {
      static KEY = { id: S.number() }
    }
    this.check(IDMustBeAString, /may only be of type string/)

    const expMsg = /this name is reserved/
    class IDCannotBeASortKeyName extends db.Model {
      static KEY = { x: S.number() }
      static SORT_KEY = { id: S.string() }
    }
    this.check(IDCannotBeASortKeyName, expMsg)
    class IDCannotBeAFieldName extends db.Model {
      static KEY = { x: S.number() }
      static FIELDS = { id: S.string() }
    }
    this.check(IDCannotBeAFieldName, expMsg)

    class SortKeyMustProvideNames extends db.Model {
      static SORT_KEY = S.number()
    }
    this.check(SortKeyMustProvideNames, /must define sort key component name/)

    class OkModel extends db.Model {
      static KEY = { id: S.string() }
      static SORT_KEY = null
    }
    OkModel.__doOneTimeModelPrep()
  }
}

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

class SimpleModel extends db.Model {}

class SimpleModelTest extends BaseTest {
  async setUp () {
    // Create new table should work
    await SimpleModel.createUnittestResource()
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
    const tempDB = require('../src/dynamodb')
    expect(tempDB.Model.createUnittestResource).toBe(undefined)
    expect(tempDB.Model.__private).toBe(undefined)
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
      const id = uuidv4()
      const model = tx.create(SimpleModel, { id })
      expect(model.id).toBe(id)
      expect(model.id).toBe(SimpleModel.__encodeCompoundValueToString(
        SimpleModel.__KEY_ORDER.partition, { id }))
      expect(model.isNew).toBe(true)
      tx.__reset() // Don't write anything, cause it will fail.
      return 321
    })
    expect(result).toBe(321) // Make sure it's done
  }

  async testGetNewModel () {
    let ret = await db.Transaction.run(async tx => {
      return tx.get(SimpleModel, uuidv4())
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

  async testNewModelParamsDeprecated () {
    const id = uuidv4()
    const model = await txCreate(SimpleModel, { id })
    expect(model.id).toBe(id)
    expect(model.params).toStrictEqual(undefined)
  }
}

class IDWithSchemaModel extends db.Model {
  static KEY = S.string().pattern(/^xyz.*$/).description(
    'any string that starts with the prefix "xyz"')
}

class CompoundIDModel extends db.Model {
  static KEY = {
    // required() does nothing because every component is required
    year: S.integer().minimum(1900).required(),
    make: S.string().minLength(3),
    upc: S.string()
  }
}

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
    expect(() => cls.key('bad')).toThrow(db.InvalidFieldError)
    const keyOrder = cls.__KEY_ORDER.partition
    expect(() => cls.__encodeCompoundValueToString(keyOrder, { id: 'X' }))
      .toThrow(db.InvalidFieldError)
    expect(cls.key('xyz').compositeID).toEqual({ id: 'xyz' })
    expect(cls.__encodeCompoundValueToString(keyOrder, { id: 'xyz' }))
      .toEqual('xyz')
  }

  async testCompoundID () {
    const compoundID = { year: 1900, make: 'Honda', upc: uuidv4() }
    const keyOrder = CompoundIDModel.__KEY_ORDER.partition
    const id = CompoundIDModel.__encodeCompoundValueToString(
      keyOrder, compoundID)
    function check (entity) {
      expect(entity.id).toBe(id)
      expect(entity.year).toBe(1900)
      expect(entity.make).toBe('Honda')
      expect(entity.upc).toBe(compoundID.upc)
    }

    check(await txCreate(CompoundIDModel, compoundID))
    check(await txGetByKey(CompoundIDModel.key(compoundID)))
    check(await txGet(CompoundIDModel, compoundID))

    expect(() => CompoundIDModel.key({})).toThrow(db.InvalidFieldError)
    expect(() => CompoundIDModel.key({
      year: undefined, // not allowed!
      make: 'Toyota',
      upc: 'nope'
    })).toThrow(db.InvalidFieldError)
    expect(() => CompoundIDModel.key({
      year: 2020,
      make: 'Toy\0ta', // no null bytes!
      upc: 'nope'
    })).toThrow(db.InvalidFieldError)
    expect(() => CompoundIDModel.key({
      year: 2040,
      make: 'need upc too'
    })).toThrow(db.InvalidFieldError)

    const msg = /incorrect number of components/
    expect(() => CompoundIDModel.__decodeCompoundValueFromString(
      keyOrder, '')).toThrow(msg)
    expect(() => CompoundIDModel.__decodeCompoundValueFromString(
      keyOrder, id + '\0')).toThrow(msg)
    expect(() => CompoundIDModel.__decodeCompoundValueFromString(
      keyOrder, '\0' + id)).toThrow(msg)

    expect(() => CompoundIDModel.key('unexpected value')).toThrow(
      db.InvalidParameterError)
  }
}

class BasicModel extends db.Model {
  static FIELDS = {
    noRequiredNoDefault: { schema: S.number(), optional: true }
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
    expect(m1.__isMutated()).toBe(true)
  }

  async testMutatedModel () {
    const m1 = await txGet(BasicModel, this.modelName)
    expect(m1.__isMutated()).toBe(false)
    m1.noRequiredNoDefault += 1
    expect(m1.__isMutated()).toBe(true)
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
  static SORT_KEY = {
    rangeKey: S.integer().minimum(1)
  }
}

class KeyTest extends BaseTest {
  async setUp () {
    await Promise.all([
      SimpleModel.createUnittestResource(),
      RangeKeyModel.createUnittestResource()
    ])
  }

  async testSortKey () {
    const id = uuidv4()
    const compositeID = { id, rangeKey: 1 }
    await txCreate(RangeKeyModel, compositeID)
    const model = await txGet(RangeKeyModel, compositeID)
    expect(model.id).toBe(id)
    expect(model.rangeKey).toBe(1)
    expect(model._sk).toBe('1')
  }

  async testValidKey () {
    SimpleModel.key(uuidv4())
    SimpleModel.key({ id: uuidv4() })
    RangeKeyModel.key({ id: uuidv4(), rangeKey: 1 })
  }

  async testInvalidKey () {
    const id = uuidv4()
    const invalidIDsForSimpleModel = [
      // these aren't even valid IDs
      1,
      '',
      String(''),
      undefined,
      {},
      [],
      { id, abc: 123 }
    ]
    for (const keyValues of invalidIDsForSimpleModel) {
      expect(() => {
        SimpleModel.key(keyValues)
      }).toThrow()
    }

    const invalidIDsForRangeModel = [
      // these have valid IDs, but are missing the required sort key
      id,
      { id },
      { id, abc: 123 },
      // has all required key, but the range key is invalid (schema mismatch)
      { id, rangeKey: '1' },
      { id, rangeKey: true },
      { id, rangeKey: 1.1 },
      { id, rangeKey: { x: 1 } },
      { id, rangeKey: [1] },
      // invalid ID and missing range key
      1,
      // missing or invalid ID
      { rangeKey: 1 },
      { id: 'bad format', rangeKey: 1 },
      // range key validation fails (right type, but too small)
      { id, rangeKey: -1 }
    ]
    for (const keyValues of invalidIDsForRangeModel) {
      expect(() => {
        RangeKeyModel.key(keyValues)
      }).toThrow()
    }
  }

  testDeprecatingLegacySyntax () {
    expect(() => {
      SimpleModel.key('id', 123)
    }).toThrow()
  }
}

class JSONModel extends db.Model {
  static FIELDS = {
    objNoDefaultNoRequired: { schema: S.object(), optional: true },
    objDefaultNoRequired: {
      schema: S.object(),
      default: { a: 1 },
      optional: true
    },
    objNoDefaultRequired: S.object(),
    objDefaultRequired: { schema: S.object(), default: {} },
    arrNoDefaultNoRequired: { schema: S.array(), optional: true },
    arrDefaultNoRequired: {
      schema: S.array(),
      default: [1, 2],
      optional: true
    },
    arrNoDefaultRequired: S.array(),
    arrDefaultRequired: { schema: S.array(), default: [] }
  }
}

class JSONModelTest extends BaseTest {
  async setUp () {
    await JSONModel.createUnittestResource()
  }

  async testRequiredFields () {
    const obj = { ab: 2 }
    const arr = [2, 1]
    const id = uuidv4()
    await expect(txGet(JSONModel, id))
      .rejects.toThrow(db.InvalidFieldError)

    await expect(txGet(JSONModel, id, model => {
      model.objNoDefaultRequired = obj
    })).rejects.toThrow(db.InvalidFieldError)

    await txGet(JSONModel, id, model => {
      model.objNoDefaultRequired = obj
      model.arrNoDefaultRequired = arr
    })

    const model = await txGet(JSONModel, id)
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
    await expect(db.__private.getWithArgs([SimpleModel], () => {})).rejects
      .toThrow(db.InvalidParameterError)
  }

  async testNoArg () {
    const invalidArgs = [undefined, {}, [], 1, '']
    for (const args of invalidArgs) {
      await expect(db.__private.getWithArgs(args, () => {})).rejects
        .toThrow(db.InvalidParameterError)
    }
  }

  async testId () {
    const params = [SimpleModel]
    await expect(db.__private.getWithArgs(params, () => {})).rejects
      .toThrow(db.InvalidParameterError)

    params.push(uuidv4())
    await expect(async () => {
      const result = await db.__private.getWithArgs(params, () => 123)
      expect(result).toBe(123)
    }).not.toThrow()

    params.push({})
    await expect(async () => {
      const result = await db.__private.getWithArgs(params, () => 234)
      expect(result).toBe(234)
    }).not.toThrow()

    params[1] = { id: uuidv4() }
    await expect(async () => {
      const result = await db.__private.getWithArgs(params, () => 23)
      expect(result).toBe(23)
    }).not.toThrow()

    params.push(1)
    await expect(db.__private.getWithArgs(params, () => {})).rejects
      .toThrow(db.InvalidParameterError)
  }

  async testKey () {
    const params = [SimpleModel.key(uuidv4())]
    expect(async () => {
      const result = await db.__private.getWithArgs(params, () => 123)
      expect(result).toBe(123)
    }).not.toThrow()

    params.push({})
    expect(async () => {
      const result = await db.__private.getWithArgs(params, () => 234)
      expect(result).toBe(234)
    }).not.toThrow()

    params.push(1)
    await expect(db.__private.getWithArgs(params, () => {})).rejects
      .toThrow(db.InvalidParameterError)
  }

  async testKeys () {
    const keys = []
    const params = [keys]
    await expect(db.__private.getWithArgs(params)).rejects
      .toThrow(db.InvalidParameterError)

    const id1 = uuidv4()
    const id2 = uuidv4()
    keys.push(SimpleModel.key(id1), SimpleModel.key(id2))
    expect(async () => {
      const result = await db.__private.getWithArgs(params,
        (key) => key.compositeID)
      expect(result).toStrictEqual([{ id: id1 }, { id: id2 }])
    }).not.toThrow()

    keys.push(1)
    await expect(db.__private.getWithArgs(params)).rejects
      .toThrow(db.InvalidParameterError)

    keys.splice(2, 1)
    params.push({})
    expect(async () => {
      const result = await db.__private.getWithArgs(params,
        (key) => key.compositeID)
      expect(result).toStrictEqual([{ id: id1 }, { id: id2 }])
    }).not.toThrow()

    params.push(1)
    await expect(db.__private.getWithArgs(params)).rejects
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
    const batcher = new db.__private.__WriteBatcher()
    const model = await txGet(BasicModel, uuidv4())
    await expect(batcher.__write(model)).rejects.toThrow()
  }

  async testDupWrite () {
    const batcher = new db.__private.__WriteBatcher()
    const model = await txGet(BasicModel, uuidv4())
    batcher.track(model)
    model.noRequiredNoDefault += 1
    expect(async () => {
      await batcher.__write(model)
    }).not.toThrow()
    await expect(batcher.__write(model)).rejects.toThrow()
  }

  async testReadonly () {
    const batcher = new db.__private.__WriteBatcher()
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

    const batcher = new db.__private.__WriteBatcher()
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
  BadModelTest,
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
