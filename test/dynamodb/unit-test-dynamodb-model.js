const uuidv4 = require('uuid').v4

const S = require('../../src/schema/src/schema')
const { BaseTest, runTests } = require('../base-unit-test')
const db = require('../db-with-field-maker')

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
      static KEY = { name: S.str }
      static FIELDS = { name: S.str }
    }
    this.check(BadModel, expMsg)

    class BadModel2 extends db.Model {
      static SORT_KEY = { name: S.str }
      static FIELDS = { name: S.str }
    }
    this.check(BadModel2, expMsg)
  }

  testReservedName () {
    class BadModel extends db.Model {
      static SORT_KEY = { isNew: S.str }
    }
    this.check(BadModel, /field name is reserved/)

    class BadModel2 extends db.Model {
      static SORT_KEY = { getField: S.str }
    }
    this.check(BadModel2, /shadows a property name/)
  }

  testIDName () {
    class SortKeyMustProvideNames extends db.Model {
      static SORT_KEY = S.double
    }
    this.check(SortKeyMustProvideNames, /must define key component name/)

    class PartitionKeyMustProvideNames extends db.Model {
      static KEY = S.double
    }
    this.check(PartitionKeyMustProvideNames, /must define key component name/)

    class OkModel extends db.Model {
      static KEY = { id: S.str }
      static SORT_KEY = null
    }
    OkModel.__doOneTimeModelPrep()

    class IDCanBePartOfACompoundPartitionKey extends db.Model {
      static KEY = { id: S.str, n: S.double }
    }
    IDCanBePartOfACompoundPartitionKey.__doOneTimeModelPrep()

    class IDDoesNotHaveToBeAString extends db.Model {
      static KEY = { id: S.double }
    }
    IDDoesNotHaveToBeAString.__doOneTimeModelPrep()

    class IDCanBeASortKeyName extends db.Model {
      static KEY = { x: S.double }
      static SORT_KEY = { id: S.str }
    }
    IDCanBeASortKeyName.__doOneTimeModelPrep()

    class IDCanBeAFieldName extends db.Model {
      static KEY = { x: S.double }
      static FIELDS = { id: S.str }
    }
    IDCanBeAFieldName.__doOneTimeModelPrep()
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
  async beforeAll () {
    // Create new table should work
    await SimpleModel.createResource()
  }

  testInvalidSetup () {
    function check (badSrc) {
      expect(() => {
        return new SimpleModel(badSrc, true, { id: uuidv4() })
      }).toThrow(/invalid item source type/)
    }
    check('nope')
    check({ isCreate: true })
  }

  async testFieldNotExtendable () {
    await expect(db.Transaction.run(tx => {
      const item = tx.create(SimpleModel, { id: uuidv4() })
      item.id.weCannotAddPropertiesToFieldsOnModels = undefined
    })).rejects.toThrow(TypeError)
  }

  async testRecreatingTable () {
    // Re-creating the same table shouldn't error out
    await SimpleModel.createResource()
  }

  async testUpdateBillingMode () {
    const setupDB = require('../../src/dynamodb/src/dynamodb')
    const dbParams = {
      dynamoDBClient: db.Model.dbClient,
      dynamoDBDocumentClient: db.Model.documentClient,
      enableDynamicResourceCreation: true,
      autoscalingClient: undefined
    }
    const onDemandDB = setupDB(dbParams)
    let CapacityModel = class extends onDemandDB.Model {}
    await CapacityModel.createResource()
    let tableDescription = await onDemandDB.Model.dbClient
      .describeTable({ TableName: CapacityModel.fullTableName })
      .promise()
    expect(tableDescription.Table.BillingModeSummary.BillingMode)
      .toBe('PAY_PER_REQUEST')

    const fakeAPI = {
      promise: async () => {
        return {
          ScalableTargets: [],
          ScalingPolicies: []
        }
      }
    }
    dbParams.autoscalingClient = {
      describeScalableTargets: () => fakeAPI,
      registerScalableTarget: () => fakeAPI,
      describeScalingPolicies: () => fakeAPI,
      putScalingPolicy: () => fakeAPI
    }
    const provisionedDB = setupDB(dbParams)
    CapacityModel = class extends provisionedDB.Model {}
    await CapacityModel.createResource()
    tableDescription = await provisionedDB.Model.dbClient
      .describeTable({ TableName: CapacityModel.fullTableName })
      .promise()
    expect(tableDescription.Table.BillingModeSummary.BillingMode)
      .toBe('PROVISIONED')
  }

  async testDebugFunctionExport () {
    // Only export in debugging
    jest.resetModules()
    const oldVal = process.env.INDEBUGGER
    process.env.INDEBUGGER = 0
    const tempDB = require('../../src/dynamodb/src/default-db')
    expect(tempDB.Model.createResource).toBe(undefined)
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
      model.someProp = 1
    }).toThrow()
  }

  async testIdImmutable () {
    const model = await txGet(SimpleModel, uuidv4())
    expect(() => {
      model.id = 'someThingElse'
    }).toThrow()
  }

  async testEventualConsistentGetParams () {
    const getParams = SimpleModel.__getParams(
      { id: '123' },
      { inconsistentRead: false })
    expect(getParams.ConsistentRead).toBe(true)
  }

  async testEventualConsistentGet () {
    const msg = uuidv4()
    const originalFunc = db.Model.__getParams
    const mock = jest.fn().mockImplementation((ignore, params) => {
      expect(params.inconsistentRead).toBe(false)
      // Hard to mock this properly,
      // so just throw with unique msg
      // and make sure it's caught outside
      throw new Error(msg)
    })
    db.Model.__getParams = mock
    const getParams = { inconsistentRead: false, createIfMissing: true }
    const fut = db.Transaction.run(async tx => {
      await tx.get(SimpleModel, uuidv4(), getParams)
    })
    await expect(fut).rejects.toThrow(Error)
    db.Model.__getParams = originalFunc
  }
}

class NewModelTest extends BaseTest {
  async testCreateModelIsNew () {
    const result = await db.Transaction.run(tx => {
      const id = uuidv4()
      const model = tx.create(SimpleModel, { id })
      expect(model.id).toBe(id)
      expect(model.id).toBe(SimpleModel.__encodeCompoundValueToString(
        SimpleModel.__keyOrder.partition, { id }))
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
      .rejects.toThrow(
        `Tried to recreate an existing model: SimpleModel _id=${id}`)
  }

  async testNewModelParamsDeprecated () {
    const id = uuidv4()
    const model = await txCreate(SimpleModel, { id })
    expect(model.id).toBe(id)
    expect(model.params).toStrictEqual(undefined)
  }
}

class IDWithSchemaModel extends db.Model {
  static KEY = {
    id: S.str.pattern(/^xyz.*$/).desc(
      'any string that starts with the prefix "xyz"')
  }
}

class CompoundIDModel extends db.Model {
  static KEY = {
    year: S.int.min(1900),
    make: S.str.min(3),
    upc: S.str
  }
}

class IDSchemaTest extends BaseTest {
  async beforeAll () {
    await IDWithSchemaModel.createResource()
    await CompoundIDModel.createResource()
  }

  async testSimpleIDWithSchema () {
    const cls = IDWithSchemaModel
    const id = 'xyz' + uuidv4()
    const m1 = await txCreate(cls, { id })
    expect(m1.id).toBe(id)
    await expect(txCreate(cls, { id: 'bad' })).rejects.toThrow(
      S.ValidationError)

    // IDs are checked when keys are created too
    expect(() => cls.key('bad')).toThrow(S.ValidationError)
    const keyOrder = cls.__keyOrder.partition
    expect(() => cls.__encodeCompoundValueToString(keyOrder, { id: 'X' }))
      .toThrow(S.ValidationError)
    expect(cls.key('xyz').encodedKeys).toEqual({ _id: 'xyz' })
    expect(cls.__encodeCompoundValueToString(keyOrder, { id: 'xyz' }))
      .toEqual('xyz')
  }

  async testCompoundID () {
    const compoundID = { year: 1900, make: 'Honda', upc: uuidv4() }
    const keyOrder = CompoundIDModel.__keyOrder.partition
    const id = CompoundIDModel.__encodeCompoundValueToString(
      keyOrder, compoundID)
    function check (entity) {
      expect(entity._id).toBe(id)
      expect(entity.year).toBe(1900)
      expect(entity.make).toBe('Honda')
      expect(entity.upc).toBe(compoundID.upc)
    }

    check(await txCreate(CompoundIDModel, compoundID))
    check(await txGetByKey(CompoundIDModel.data(compoundID)))
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
      keyOrder, '', 'fake')).toThrow(msg)
    expect(() => CompoundIDModel.__decodeCompoundValueFromString(
      keyOrder, id + '\0', 'fake')).toThrow(msg)
    expect(() => CompoundIDModel.__decodeCompoundValueFromString(
      keyOrder, '\0' + id, 'fake')).toThrow(msg)

    expect(() => CompoundIDModel.key('unexpected value')).toThrow(
      db.InvalidParameterError)
  }
}

class BasicModel extends db.Model {
  static FIELDS = {
    noRequiredNoDefault: S.double.optional()
  }
}

class WriteTest extends BaseTest {
  async beforeAll () {
    await BasicModel.createResource()
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
      const awsName = model.getField('noRequiredNoDefault').__awsName
      expect(params[CONDITION_EXPRESSION_STR]).toContain(
        awsName + '=:_')
      expect(params.ExpressionAttributeValues).toHaveProperty(
        ':_0')
    })
  }

  async testNoAccessProperty () {
    // Building block for strong Transaction isolation levels
    const m1 = await txGet(BasicModel, this.modelName)
    let params = m1.__updateParams()
    expect(params.ConditionExpression).toBe('attribute_exists(#_id)')
    expect(params).not.toHaveProperty(UPDATE_EXPRESSION_STR)

    // Make sure no fields are "accessed" while getting params
    m1.__putParams()
    params = m1.__updateParams()
    expect(params.ConditionExpression).toBe('attribute_exists(#_id)')
    expect(params).not.toHaveProperty(UPDATE_EXPRESSION_STR)
    expect(m1.__cached_attrs.id.accessed).toBe(false)
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
        .toContain('REMOVE ' + model.getField(propName).__awsName)
    })

    // Read and check again
    model = await txGet(BasicModel, model.id)
    expect(model.noRequiredNoDefault).toBe(undefined)
  }

  async testNoLockOption () {
    const model = await txGet(BasicModel, this.modelName)
    model.getField('noRequiredNoDefault').incrementBy(1)
    expect(model.__updateParams().ExpressionAttributeNames)
      .not.toContain('noRequiredNoDefault')
  }

  async testPutNoLock () {
    const model = await txGet(BasicModel, this.modelName)
    model.getField('noRequiredNoDefault').incrementBy(1)
    expect(model.__putParams().ExpressionAttributeNames)
      .not.toContain('noRequiredNoDefault')
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
  async beforeAll () {
    await BasicModel.createResource()
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
    expect(m1.__conditionCheckParams().ConditionExpression)
      .toBe('attribute_exists(#_id)')
  }

  async testReadonlyModel () {
    const m1 = await txGet(BasicModel, this.modelName)
    m1.noRequiredNoDefault // eslint-disable-line no-unused-expressions
    const awsName = m1.getField('noRequiredNoDefault').__awsName
    expect(m1.__conditionCheckParams()).toHaveProperty('ConditionExpression',
      `attribute_exists(#_id) AND attribute_not_exists(${awsName})`)
  }
}

class RangeKeyModel extends db.Model {
  static SORT_KEY = {
    rangeKey: S.int.min(1)
  }

  static FIELDS = {
    n: S.int
  }
}

class KeyTest extends BaseTest {
  async beforeAll () {
    await Promise.all([
      SimpleModel.createResource(),
      RangeKeyModel.createResource()
    ])
  }

  async testGetNoCreateIfMissingWithExcessFields () {
    const fut = db.Transaction.run(async tx => {
      // can't specify field like "n" when reading unless we're doing a
      // createIfMissing=true
      await tx.get(RangeKeyModel, { id: uuidv4(), rangeKey: 3, n: 3 })
    })
    await expect(fut).rejects.toThrow(/received non-key fields/)
  }

  testDataKey () {
    const id = uuidv4()
    const data = RangeKeyModel.data({ id, rangeKey: 1, n: 5 })
    const key = data.key
    expect(key.keyComponents.id).toBe(id)
    expect(key.keyComponents.rangeKey).toBe(1)
    expect(data.data.n).toBe(5)
  }

  async testGetWithWrongType () {
    await expect(db.Transaction.run(async tx => {
      await tx.get(RangeKeyModel.key({ id: uuidv4(), rangeKey: 2 }), {
        createIfMissing: true
      })
    })).rejects.toThrow(/must pass a Data/)

    await expect(db.Transaction.run(async tx => {
      await tx.get(RangeKeyModel.data({ id: uuidv4(), rangeKey: 2, n: 3 }))
    })).rejects.toThrow(/must pass a Key/)
  }

  async testSortKey () {
    async function check (id, rangeKey, n, create = true) {
      const encodedKeys = { id, rangeKey }
      if (create) {
        await txCreate(RangeKeyModel, { ...encodedKeys, n })
      }
      const model = await txGet(RangeKeyModel, encodedKeys)
      expect(model.id).toBe(id)
      expect(model.rangeKey).toBe(rangeKey)
      expect(model._sk).toBe(rangeKey.toString())
      expect(model.n).toBe(n)
    }

    const id1 = uuidv4()
    await check(id1, 1, 0)

    // changing the sort key means we're working with a different item
    await check(id1, 2, 1)
    await check(id1, 1, 0, false)

    // changing the partition key but not the sort key also means we're working
    // with a different item
    const id2 = uuidv4()
    await check(id2, 1, 2)
    await check(id2, 2, 3)
    await check(id1, 1, 0, false)
    await check(id1, 2, 1, false)

    // should be able to update fields in a model with a sort key
    await db.Transaction.run(async tx => {
      await tx.update(RangeKeyModel, { id: id1, rangeKey: 1, n: 0 }, { n: 99 })
    })
    await check(id1, 1, 99, false)
    // but not the sort key itself
    // this throws because no such item exists:
    await expect(db.Transaction.run(async tx => {
      await tx.update(RangeKeyModel, { id: id1, rangeKey: 9, n: 0 }, { n: 99 })
    })).rejects.toThrow()
    // these last two both throw because we can't modify key values
    await expect(db.Transaction.run(async tx => {
      const x = await tx.get(RangeKeyModel, { id: id1, rangeKey: 1 })
      x.rangeKey = 2
    })).rejects.toThrow(/rangeKey is immutable/)
    await expect(db.Transaction.run(async tx => {
      await tx.update(RangeKeyModel, { id: id1, rangeKey: 1 }, { rangeKey: 2 })
    })).rejects.toThrow(/must not contain key fields/)
    await expect(db.Transaction.run(async tx => {
      const x = await tx.get(RangeKeyModel, { id: id1, rangeKey: 1 })
      x.id = uuidv4()
    })).rejects.toThrow(/id is immutable/)
    await expect(db.Transaction.run(async tx => {
      await tx.update(RangeKeyModel, { id: id1, rangeKey: 1 }, { id: id2 })
    })).rejects.toThrow(/must not contain key fields/)
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
    objNoDefaultNoRequired: S.obj().optional(),
    objDefaultNoRequired: S.obj({
      a: S.int
    }).default({ a: 1 }).optional(),
    objNoDefaultRequired: S.obj({
      ab: S.int.optional(),
      cd: S.arr(S.int).optional()
    }),
    objDefaultRequired: S.obj().default({}),
    arrNoDefaultNoRequired: S.arr().optional(),
    arrDefaultNoRequired: S.arr().default([1, 2]).optional(),
    arrNoDefaultRequired: S.arr(S.obj({
      cd: S.int.optional(),
      bc: S.int.optional()
    })),
    arrDefaultRequired: S.arr().default([])
  }
}

class JSONModelTest extends BaseTest {
  async beforeAll () {
    await JSONModel.createResource()
  }

  async testRequiredFields () {
    const obj = { ab: 2 }
    const arr = [{ cd: 2 }, { cd: 1 }]
    async function check (input) {
      input.id = uuidv4()
      await expect(txGet(JSONModel, input)).rejects.toThrow(
        /missing required value/)
    }
    await check({})
    await check({ objNoDefaultRequired: obj })
    await check({ arrNoDefaultRequired: arr })

    const id = uuidv4()
    async function checkOk (input) {
      const model = await txGet(JSONModel, input)
      expect(model.id).toBe(id)
      expect(model.objNoDefaultRequired).toEqual(obj)
      expect(model.arrNoDefaultRequired).toEqual(arr)
      expect(model.objNoDefaultNoRequired).toBe(undefined)
      expect(model.arrNoDefaultNoRequired).toBe(undefined)
      expect(model.objDefaultNoRequired).toEqual({ a: 1 })
      expect(model.arrDefaultNoRequired).toEqual([1, 2])
      expect(model.objDefaultRequired).toEqual({})
      expect(model.arrDefaultRequired).toEqual([])
    }
    await checkOk({ id, objNoDefaultRequired: obj, arrNoDefaultRequired: arr })
    await checkOk({ id }) // just getting, not creating
  }

  async testDeepUpdate () {
    const obj = { cd: [] }
    const arr = [{}]
    const id = uuidv4()
    const data = { id, objNoDefaultRequired: obj, arrNoDefaultRequired: arr }
    await txGet(JSONModel, data, model => {
      expect(model.isNew).toBe(true)
    })

    await txGet(JSONModel, id, model => {
      obj.cd.push(1)
      model.objNoDefaultRequired.cd.push(1)
      arr[0].bc = 32
      model.arrNoDefaultRequired[0].bc = 32
    })

    await txGet(JSONModel, id, model => {
      expect(model.objNoDefaultRequired).toStrictEqual(obj)
      expect(model.arrNoDefaultRequired).toStrictEqual(arr)
    })
  }

  async testToJson () {
    const id = uuidv4()
    const data = {
      id,
      objNoDefaultNoRequired: {},
      objDefaultNoRequired: { a: 1 },
      objNoDefaultRequired: { ab: 12, cd: [23] },
      objDefaultRequired: { a: '2' },
      arrDefaultNoRequired: [2, 3],
      arrNoDefaultRequired: [{ cd: 2 }],
      arrDefaultRequired: []
    }
    const model = await db.Transaction.run(async tx => {
      return tx.get(JSONModel, data, { createIfMissing: true })
    })
    expect(model.toJSON()).toEqual(data)
  }
}

class GetArgsParserTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await SimpleModel.createResource()
  }

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
    const result = await db.__private.getWithArgs(params,
      (keys) => keys.map(key => key.encodedKeys))
    expect(result).toStrictEqual([{ _id: id1 }, { _id: id2 }])

    keys.push(1)
    await expect(db.__private.getWithArgs(params)).rejects
      .toThrow(db.InvalidParameterError)

    keys.splice(2, 1)
    params.push({})
    const result1 = await db.__private.getWithArgs(params,
      (keys) => keys.map(key => key.encodedKeys))
    expect(result1).toStrictEqual([{ _id: id1 }, { _id: id2 }])

    params.push(1)
    await expect(db.__private.getWithArgs(params)).rejects
      .toThrow(db.InvalidParameterError)
  }
}

class WriteBatcherTest extends BaseTest {
  async beforeAll () {
    await BasicModel.createResource()
    this.modelNames = [uuidv4(), uuidv4()]
    const promises = this.modelNames.map(name => {
      return txGet(BasicModel, name, (m) => {
        m.noRequiredNoDefault = 0
      })
    })
    await Promise.all(promises)
  }

  async afterEach () {
    jest.restoreAllMocks()
  }

  async testUntrackedWrite () {
    const batcher = new db.__private.__WriteBatcher()
    const model = await txGet(BasicModel, uuidv4())
    expect(() => batcher.__write(model)).toThrow()
  }

  async testDupWrite () {
    const batcher = new db.__private.__WriteBatcher()
    const model = await txGet(BasicModel, uuidv4())
    batcher.track(model)
    model.noRequiredNoDefault += 1
    batcher.__write(model)
    expect(() => batcher.__write(model)).toThrow()
  }

  async testReadonly () {
    const batcher = new db.__private.__WriteBatcher()
    const model1 = await txGet(BasicModel, this.modelNames[0])
    const model2 = await txGet(BasicModel, this.modelNames[1])
    batcher.track(model1)
    batcher.track(model2)
    model1.noRequiredNoDefault = model2.noRequiredNoDefault + 1

    const msg = uuidv4()
    const mock = jest.spyOn(batcher.documentClient, 'transactWrite')
      .mockImplementation(data => {
        const update = data.TransactItems[0].Update
        // we never read the old value on model1, so our update should NOT be
        // conditioned on the old value
        if (update) {
          expect(update.ConditionExpression).toBe('attribute_exists(#_id)')
        }
        const awsName = model1.getField('noRequiredNoDefault').__awsName
        expect(update.UpdateExpression).toBe(`SET ${awsName}=:_0`)

        const condition = data.TransactItems[1].ConditionCheck
        expect(Object.keys(condition.ExpressionAttributeValues).length).toEqual(1)
        expect(condition.ConditionExpression)
          .toBe(`attribute_exists(#_id) AND ${awsName}=:_0`)
        throw new Error(msg)
      })
    await expect(batcher.commit(true)).rejects.toThrow(msg)
    expect(mock).toHaveBeenCalledTimes(1)
  }

  async testReservedAttributeName () {
    // AWS reserves a lot of names like the ones used as field names here; we
    // should be able to use them anyway (thanks to ExpressionAttributeNames)
    class ReservedAttrName extends db.Model {
      static FIELDS = { items: S.obj(), count: S.int, token: S.str }
    }
    await ReservedAttrName.createResource()
    await db.Transaction.run(tx => {
      tx.create(ReservedAttrName, {
        id: uuidv4(), items: {}, count: 0, token: 'x'
      })
    })
  }

  async testExceptionParser () {
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

    let itemSourceCreate
    await db.Transaction.run(tx => {
      const item = tx.create(IDWithSchemaModel, { id: 'xyz' + uuidv4() })
      itemSourceCreate = item.__src
    })

    const batcher = new db.__private.__WriteBatcher()
    batcher.track({
      __fullTableName: 'sharedlibTestModel',
      tableName: 'TestModel',
      _id: '123',
      __src: itemSourceCreate
    })
    batcher.__extractError({}, response)
    expect(response.error).toBe(undefined)

    try {
      batcher.__extractError({}, {
        httpResponse: {
          body: JSON.stringify({ oops: 'unexpected response structure' })
        }
      })
    } catch (e) {
      expect(e.message).toContain('error body missing reasons')
    }

    const item = { _id: { S: '123' } }
    reasons.push({
      Code: 'ConditionalCheckFailed',
      Item: item
    })
    const request = {
      params: {
        TransactItems: [{
          Put: {
            Item: item,
            TableName: 'sharedlibTestModel'
          }
        }]
      }
    }
    response.error = undefined
    batcher.__extractError(request, response)
    expect(response.error.message)
      .toBe('Tried to recreate an existing model: sharedlibTestModel _id=123')

    batcher.__allModels[0]._sk = '456'
    request.params.TransactItems = [
      {
        Update: {
          Key: { _id: { S: '123' }, _sk: { S: '456' } },
          TableName: 'sharedlibTestModel'
        }
      }
    ]
    response.error = undefined
    batcher.__extractError(request, response)
    expect(response.error.message)
      .toBe([
        'Tried to recreate an existing model: ',
        'sharedlibTestModel _id=123 _sk=456'].join(''))

    response.error = undefined
    batcher.__allModels[0].__src = 'something else'
    batcher.__extractError(request, response)
    expect(response.error).toBe(undefined)

    reasons[0].Code = 'anything else'
    response.error = undefined
    batcher.__extractError({}, response)
    expect(response.error).toBe(undefined)
  }

  async testModelAlreadyExistsError () {
    // Single item transactions
    const id = uuidv4()
    await txCreate(BasicModel, { id })
    let fut = txCreate(BasicModel, { id })
    await expect(fut).rejects.toThrow(db.ModelAlreadyExistsError)

    // Multi-items transactions
    fut = db.Transaction.run(async (tx) => {
      tx.create(BasicModel, { id })
      tx.create(BasicModel, { id: uuidv4() })
    })
    await expect(fut).rejects.toThrow(db.ModelAlreadyExistsError)
  }

  async testInvalidModelUpdateError () {
    const id = uuidv4()
    let fut = db.Transaction.run(async (tx) => {
      tx.update(BasicModel, { id }, { noRequiredNoDefault: 1 })
    })
    await expect(fut).rejects.toThrow(db.InvalidModelUpdateError)

    fut = db.Transaction.run(async (tx) => {
      tx.create(BasicModel, { id: uuidv4() })
      tx.update(BasicModel, { id }, { noRequiredNoDefault: 1 })
    })
    await expect(fut).rejects.toThrow(db.InvalidModelUpdateError)
  }

  /**
   * Verify creating a model with invalid key fails
   */
  async testInvalidKey () {
    let createPromise = db.Transaction.run(async tx => {
      tx.create(BasicModel, { id: { test: 'not valid schema' } })
    })
    await expect(createPromise).rejects.toThrow(S.ValidationError)

    createPromise = db.Transaction.run(async tx => {
      return tx.get(BasicModel, { id: { test: 'not valid schema' } }, { createIfMissing: true })
    })

    await expect(createPromise).rejects.toThrow(S.ValidationError)
  }

  /**
   * Verify modifying keyparts is not allowed
   */
  async testMutatingKeyparts () {
    await CompoundIDModel.createResource()
    const compoundID = { year: 1900, make: 'Honda', upc: uuidv4() }
    let createPromise = db.Transaction.run(async tx => {
      const model = tx.create(CompoundIDModel, compoundID)
      model.year = 1901
    })
    await expect(createPromise).rejects.toThrow(db.InvalidFieldError)

    await db.Transaction.run(async tx => {
      return tx.create(CompoundIDModel, compoundID)
    })

    createPromise = db.Transaction.run(async tx => {
      const model = await tx.get(CompoundIDModel, compoundID)
      model.year = 1901
    })

    await expect(createPromise).rejects.toThrow(db.InvalidFieldError)
  }
}

class DefaultsTest extends BaseTest {
  /**
   * Verify that nested defaults are applied
   * to saved models
   */
  async testNestedDefaultsOnSave () {
    class NestedDefaultsModel extends db.Model {
      static FIELDS = {
        arr: S.arr(S.obj({
          int: S.int,
          str: S.str.default('butterfly')
        })),
        obj: S.obj({
          int: S.int.default(2),
          str: S.str.default('shoe')
        }).default({})
      }
    }
    await NestedDefaultsModel.createResource()
    const id = uuidv4()

    await db.Transaction.run(async tx => {
      tx.create(NestedDefaultsModel, {
        id: id,
        arr: [{ int: 2 }, { int: 3 }]
      })
    })

    await db.Transaction.run(async tx => {
      const result = await tx.get(NestedDefaultsModel, id)
      expect(result.arr).toEqual([
        {
          int: 2,
          str: 'butterfly'
        },
        {
          int: 3,
          str: 'butterfly'
        }
      ])

      // obj defaults should be applied top-down
      expect(result.obj).toEqual({
        int: 2,
        str: 'shoe'
      })
    })
  }

  /**
   * Verify that nested schemas with 'undefined' as default
   * do not pass validation
   */
  async testNestedDefaultValidation () {
    class NestedDefaultsModel extends db.Model {
      static FIELDS = {
        arr: S.arr(S.obj({
          str: S.str.default(undefined)
        }))
      }
    }

    expect(async () => await NestedDefaultsModel.createResource())
      .rejects
      .toThrow('No default value can be set to undefined')
  }

  /**
   * Verify that nested defaults are applied
   * when retrieving models
   */
  async testNestedDefaultsOnGet () {
    const fields = {
      arr: S.arr(S.obj({
        int: S.int,
        str: S.str.default('butterfly')
      }))
    }

    class NestedDefaultsModel extends db.Model {
      static FIELDS = fields
    }

    await NestedDefaultsModel.createResource()
    const id = uuidv4()

    await db.Transaction.run(async tx => {
      tx.create(NestedDefaultsModel, {
        id: id,
        arr: [{ int: 2 }, { int: 3 }]
      })
    })

    fields.arr.itemsSchema.__isLocked = false
    fields.arr.itemsSchema.prop('newField', S.str.default('newDefault'))
    delete NestedDefaultsModel.__setupDone
    NestedDefaultsModel.__doOneTimeModelPrep()

    await db.Transaction.run(async tx => {
      const result = await tx.get(NestedDefaultsModel, id)
      expect(result.arr).toEqual([
        {
          int: 2,
          str: 'butterfly',
          newField: 'newDefault'
        },
        {
          int: 3,
          str: 'butterfly',
          newField: 'newDefault'
        }
      ])
    })
  }
}

class OptDefaultModelTest extends BaseTest {
  async testFieldWhichIsBothOptionalAndDefault () {
    class OptDefaultModel extends db.Model {
      static get FIELDS () {
        return {
          def: S.int.default(7),
          opt: S.int.optional(),
          defOpt: S.int.default(7).optional()
        }
      }
    }
    await OptDefaultModel.createResource()

    function check (obj, def, opt, defOpt, def2, opt2, defOpt2) {
      expect(obj.def).toBe(def)
      expect(obj.opt).toBe(opt)
      expect(obj.defOpt).toBe(defOpt)
      if (def2) {
        expect(obj.def2).toBe(def2)
        expect(obj.opt2).toBe(opt2)
        expect(obj.defOpt2).toBe(defOpt2)
      }
    }

    const idSpecifyNothing = uuidv4()
    const idSpecifyAll = uuidv4()
    const idUndef = uuidv4()
    await db.Transaction.run(tx => {
      // can just use the defaults (specify no field values)
      check(tx.create(OptDefaultModel, { id: idSpecifyNothing }),
        7, undefined, 7)

      // can use our own values (specify all field values)
      check(tx.create(OptDefaultModel, {
        id: idSpecifyAll,
        def: 1,
        opt: 2,
        defOpt: 3
      }), 1, 2, 3)

      // optional fields with a default can still be omitted from the db (i.e.,
      // assigned a value of undefined)
      check(tx.create(OptDefaultModel, {
        id: idUndef,
        defOpt: undefined
      }), 7, undefined, undefined)
    })

    // verify that these are all properly stored to the database
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultModel, idSpecifyNothing), 7, undefined, 7)
      check(await tx.get(OptDefaultModel, idSpecifyAll), 1, 2, 3)
      check(await tx.get(OptDefaultModel, idUndef), 7, undefined, undefined)
    })

    // add a new set of fields (normally we'd do this on the same model, but
    // for the test we do it in a new model (but SAME TABLE) because one-time
    // setup is already done for the other model)
    class OptDefaultModel2 extends db.Model {
      static tableName = OptDefaultModel.name
      static FIELDS = {
        ...OptDefaultModel.FIELDS,
        def2: S.int.default(8),
        opt2: S.int.optional(),
        defOpt2: S.int.default(8).optional()
      }
    }
    await OptDefaultModel2.createResource()

    // the default value for new fields isn't stored in the db yet (old items
    // have not been changed yet)
    let fut = db.Transaction.run(async tx => {
      await tx.update(OptDefaultModel2,
        { id: idSpecifyNothing, def2: 8 }, { def: 1 })
    })
    await expect(fut).rejects.toThrow(/outdated \/ invalid conditions/)

    // we can (ONLY) use update() on defaults that have been written to the db
    await db.Transaction.run(async tx => {
      await tx.update(OptDefaultModel2,
        { id: idSpecifyNothing, def: 7 }, { opt2: 11 })
    })

    // blind updates are only partial, so they won't populate a new default
    // field unless explicitly given a value for it
    fut = db.Transaction.run(async tx => {
      await tx.update(OptDefaultModel2,
        { id: idSpecifyNothing, def2: 8 }, { def: 2 })
    })
    await expect(fut).rejects.toThrow(/outdated \/ invalid conditions/)

    // verify that these are all in the proper state when accessing old items;
    // also, accessing the item populates the default value for the new field
    // which triggers a database write!
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultModel2, idSpecifyNothing),
        7, undefined, 7,
        8, 11, undefined)
    })
    await db.Transaction.run(async tx => {
      // verify the db was updated by doing a blind update dependent on it
      await tx.update(OptDefaultModel2,
        { id: idSpecifyNothing, def2: 8 }, { def: 100 })
    })
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultModel2, idSpecifyNothing),
        100, undefined, 7, 8, 11, undefined)
    })

    // accessing and modifying an old item will also write the new defaults to
    // the db
    await db.Transaction.run(async tx => {
      const item = await tx.get(OptDefaultModel2, idUndef)
      check(item, 7, undefined, undefined,
        8, undefined, undefined)
      item.def = 3
    })
    await db.Transaction.run(async tx => {
      // verify the db was updated by doing a blind update dependent on it
      await tx.update(OptDefaultModel2,
        { id: idUndef, def: 3, def2: 8 }, { opt2: 101 })
    })
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultModel2, idUndef),
        3, undefined, undefined, 8, 101, undefined)
    })
  }
}

class OptionalFieldConditionTest extends BaseTest {
  async testOptFieldCondition () {
    class OptNumModel extends db.Model {
      static get FIELDS () {
        return {
          n: S.int.optional()
        }
      }
    }
    await OptNumModel.createResource()

    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(OptNumModel, { id })
    })
    await db.Transaction.run(async tx => {
      const item = await tx.get(OptNumModel, id)
      if (item.n === undefined) {
        item.n = 5
      }
      const field = item.getField('n')
      const [condition, vals] = field.__conditionExpression(':_1')
      expect(condition).toBe(`attribute_not_exists(${field.__awsName})`)
      expect(vals).toEqual({})
    })
  }
}

class TTLModel extends db.Model {
  static FIELDS = {
    expirationTime: S.int,
    doubleTime: S.double,
    notTime: S.str.optional(),
    optionalTime: S.int.optional()
  }

  static EXPIRE_EPOCH_FIELD = 'expirationTime'
}

class NoTTLModel extends TTLModel {
  static EXPIRE_EPOCH_FIELD = undefined
}

class TTLTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await TTLModel.createResource()
    await NoTTLModel.createResource()
  }

  async testTTL () {
    const id = uuidv4()
    const currentTime = Math.floor(new Date().getTime() / 1000)
    await db.Transaction.run(tx => {
      tx.create(TTLModel, {
        id,
        expirationTime: currentTime + 1,
        doubleTime: 1
      })
    })

    await new Promise((resolve, reject) => {
      setTimeout(resolve, 2000)
    })

    const model = await db.Transaction.run(tx => {
      return tx.get(TTLModel, id)
    })
    expect(model).toBeUndefined()
  }

  async testCFResource () {
    expect(Object.values(TTLModel.__getResourceDefinitions())[0].Properties)
      .toHaveProperty('TimeToLiveSpecification')
  }

  async testConfigValidation () {
    const Cls1 = class extends TTLModel {
      static EXPIRE_EPOCH_FIELD = 'notTime'
    }
    expect(() => {
      Cls1.__getResourceDefinitions()
    }).toThrow('must refer to an integer or double field')

    const Cls2 = class extends TTLModel {
      static EXPIRE_EPOCH_FIELD = 'optionalTime'
    }
    Cls2.__getResourceDefinitions() // works ok

    const Cls3 = class extends TTLModel {
      static EXPIRE_EPOCH_FIELD = 'doubleTime'
    }
    expect(() => {
      Cls3.__getResourceDefinitions()
    }).not.toThrow()

    const Cls4 = class extends TTLModel {
      static EXPIRE_EPOCH_FIELD = 'invalid'
    }
    expect(() => {
      Cls4.__getResourceDefinitions()
    }).toThrow('EXPIRE_EPOCH_FIELD must refer to an existing field')
  }

  async testExpiration () {
    const currentTime = Math.ceil(new Date().getTime() / 1000)
    const result = await db.Transaction.run(async tx => {
      const model = tx.create(TTLModel,
        { id: uuidv4(), expirationTime: 0, doubleTime: 0 })
      // No value, no expiration
      expect(model.__hasExpired).toBe(false)

      // Older then 5 years, no expiration
      model.expirationTime = 120000000
      expect(model.__hasExpired).toBe(false)

      // No value, no expiration
      expect(model.__hasExpired).toBe(false)

      // Expired
      model.expirationTime = currentTime - 1000
      expect(model.__hasExpired).toBe(true)

      // Not yet
      model.expirationTime = currentTime + 1000
      expect(model.__hasExpired).toBe(false)

      // TTL not enabled, no expiration
      const model1 = tx.create(NoTTLModel,
        { id: uuidv4(), expirationTime: 0, doubleTime: 0 })
      model1.expirationTime = currentTime - 1000
      expect(model1.__hasExpired).toBe(false)
      return 1122
    })
    expect(result).toBe(1122) // Proof that the tx ran
  }

  async testExpiredModel () {
    // Expired model should be hidden
    const currentTime = Math.ceil(new Date().getTime() / 1000)

    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(NoTTLModel, {
        id,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
    })

    // Turn on ttl locally now
    NoTTLModel.EXPIRE_EPOCH_FIELD = 'expirationTime'

    // if not createIfMissing, nothing should be returned
    let model = await db.Transaction.run(tx => {
      return tx.get(NoTTLModel, id)
    })
    expect(model).toBeUndefined()

    // if createIfMissing, a new model should be returned
    model = await db.Transaction.run(tx => {
      return tx.get(NoTTLModel,
        { id, expirationTime: currentTime + 10000, doubleTime: 111 },
        { createIfMissing: true })
    })
    expect(model.isNew).toBe(true)

    model = await db.Transaction.run(tx => {
      return tx.get(NoTTLModel, id)
    })
    expect(model.doubleTime).toBe(111)
    expect(model.isNew).toBe(false)

    NoTTLModel.EXPIRE_EPOCH_FIELD = undefined
  }

  async testOverrideExpiredModel () {
    // When blind write to a model with TTL enabled, the condition must take
    // expired but not yet deleted models into account, and don't fail the tx
    const currentTime = Math.ceil(new Date().getTime() / 1000)

    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(NoTTLModel, {
        id,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
    })
    // Turn on ttl locally now
    NoTTLModel.EXPIRE_EPOCH_FIELD = 'expirationTime'

    await db.Transaction.run(tx => {
      tx.create(NoTTLModel,
        { id, expirationTime: currentTime + 1000, doubleTime: 111 })
    })

    const model = await db.Transaction.run(tx => {
      return tx.get(NoTTLModel, id)
    })
    expect(model.doubleTime).toBe(111)

    NoTTLModel.EXPIRE_EPOCH_FIELD = undefined
  }

  async testBatchGetExpired () {
    const currentTime = Math.ceil(new Date().getTime() / 1000)

    const id = uuidv4()
    const id2 = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(NoTTLModel, {
        id,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
      tx.create(NoTTLModel, {
        id: id2,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
    })
    // Turn on ttl locally now
    NoTTLModel.EXPIRE_EPOCH_FIELD = 'expirationTime'

    const result = await db.Transaction.run(tx => {
      return tx.get([
        NoTTLModel.key(id), NoTTLModel.key(uuidv4())
      ], { inconsistentRead: false })
    })
    expect(result).toStrictEqual([undefined, undefined])

    const result1 = await db.Transaction.run(tx => {
      return tx.get([
        NoTTLModel.key(id), NoTTLModel.key(uuidv4())
      ], { inconsistentRead: true })
    })
    expect(result1).toStrictEqual([undefined, undefined])

    const result2 = await db.Transaction.run(tx => {
      return tx.get([
        NoTTLModel.data({
          id,
          expirationTime: currentTime - 10000,
          doubleTime: 1
        }),
        NoTTLModel.data({
          id: id2,
          expirationTime: currentTime - 10000,
          doubleTime: 1
        })
      ], { inconsistentRead: false, createIfMissing: true })
    })
    expect(result2.length).toBe(2)
    expect(result2[0].id).toBe(id)
    expect(result2[1].id).toBe(id2)

    NoTTLModel.EXPIRE_EPOCH_FIELD = undefined
  }
}

class SnapshotTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await JSONModel.createResource()
    this.modelID = uuidv4()
    await db.Transaction.run(async tx => {
      await tx.get(JSONModel, {
        id: this.modelID,
        objNoDefaultRequired: { ab: 11 },
        arrNoDefaultRequired: []
      }, { createIfMissing: true })
    })
  }

  async testGetNewModel () {
    const id = uuidv4()
    const result = await db.Transaction.run(async tx => {
      const m = await tx.get(JSONModel,
        {
          id,
          objNoDefaultRequired: { ab: 123 },
          arrNoDefaultRequired: [{ cd: 12 }]
        },
        { createIfMissing: true })
      expect(m.getField('arrNoDefaultNoRequired').accessed).toBe(false)
      const data = {
        before: m.getSnapshot({ initial: true, dbKeys: true }),
        after: m.getSnapshot({ dbKeys: true })
      }
      // getSnapshot should not mark fields as accessed so optimistic lock in
      // TX is not affected.
      expect(m.getField('arrNoDefaultNoRequired').accessed).toBe(false)
      return data
    })
    expect(result).toStrictEqual({
      before: {
        _id: undefined,
        arrDefaultNoRequired: undefined,
        arrDefaultRequired: undefined,
        arrNoDefaultNoRequired: undefined,
        arrNoDefaultRequired: undefined,
        objDefaultNoRequired: undefined,
        objDefaultRequired: undefined,
        objNoDefaultNoRequired: undefined,
        objNoDefaultRequired: undefined
      },
      after: {
        _id: id,
        arrDefaultNoRequired: [1, 2],
        arrDefaultRequired: [],
        arrNoDefaultNoRequired: undefined,
        arrNoDefaultRequired: [{ cd: 12 }],
        objDefaultNoRequired: { a: 1 },
        objDefaultRequired: {},
        objNoDefaultNoRequired: undefined,
        objNoDefaultRequired: { ab: 123 }
      }
    })
  }

  async testGetExistingModel () {
    const result = await db.Transaction.run(async tx => {
      const m = await tx.get(JSONModel, this.modelID)
      return {
        before: m.getSnapshot({ initial: true, dbKeys: true }),
        after: m.getSnapshot({ dbKeys: true })
      }
    })
    const expectation = {
      _id: this.modelID,
      arrDefaultNoRequired: [1, 2],
      arrDefaultRequired: [],
      arrNoDefaultNoRequired: undefined,
      arrNoDefaultRequired: [],
      objDefaultNoRequired: { a: 1 },
      objDefaultRequired: {},
      objNoDefaultNoRequired: undefined,
      objNoDefaultRequired: { ab: 11 }
    }
    expect(result.before).toStrictEqual(expectation)
    expect(result.after).toStrictEqual(expectation)
  }

  async testRangeKey () {
    await db.Transaction.run(async tx => {
      const id = uuidv4()
      const m = await tx.get(RangeKeyModel, { id, rangeKey: 1, n: 1 }, { createIfMissing: true })
      expect(m.getSnapshot({ initial: true, dbKeys: true })).toStrictEqual({
        _id: undefined,
        _sk: undefined,
        n: undefined
      })
      expect(m.getSnapshot({ initial: true })).toStrictEqual({
        id: undefined,
        rangeKey: undefined,
        n: undefined
      })
      expect(m.getSnapshot({ dbKeys: true })).toStrictEqual({
        _id: id,
        _sk: '1',
        n: 1
      })
      expect(m.getSnapshot({})).toStrictEqual({
        id: id,
        rangeKey: 1,
        n: 1
      })
    })
  }
}

class UniqueKeyListTest extends BaseTest {
  testDedup () {
    const id = uuidv4()
    const keys = new db.UniqueKeyList(NoTTLModel.key(id))
    keys.push(NoTTLModel.key(id), NoTTLModel.key(uuidv4()))
    expect(keys.length).toBe(2)
    keys.push(NoTTLModel.key(id))
    expect(keys.length).toBe(2)
  }

  async testGet () {
    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(SimpleModel, { id })
    })
    const keys = new db.UniqueKeyList(SimpleModel.key(id))
    const result = await db.Transaction.run(tx => {
      return tx.get(keys)
    })
    expect(result[0].id).toBe(id)
  }
}

runTests(
  BadModelTest,
  ConditionCheckTest,
  DefaultsTest,
  ErrorTest,
  GetArgsParserTest,
  IDSchemaTest,
  JSONModelTest,
  KeyTest,
  NewModelTest,
  OptDefaultModelTest,
  OptionalFieldConditionTest,
  SimpleModelTest,
  SnapshotTest,
  TTLTest,
  WriteBatcherTest,
  WriteTest,
  UniqueKeyListTest
)
