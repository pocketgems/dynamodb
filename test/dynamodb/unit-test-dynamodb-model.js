const uuidv4 = require('uuid').v4

const AWSError = require('../../src/dynamodb/src/aws-error')
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
    class BadExample extends db.Model {
      static KEY = {}
    }
    this.check(BadExample, /at least one partition key field/)
    class BadModel2 extends db.Model {
      static KEY = null
    }
    this.check(BadModel2, /partition key is required/)
  }

  testDuplicateField () {
    const expMsg = /more than once/
    class BadExample extends db.Model {
      static KEY = { name: S.str }
      static FIELDS = { name: S.str }
    }
    this.check(BadExample, expMsg)

    class BadModel2 extends db.Model {
      static SORT_KEY = { name: S.str }
      static FIELDS = { name: S.str }
    }
    this.check(BadModel2, expMsg)
  }

  testReservedName () {
    class BadExample extends db.Model {
      static SORT_KEY = { isNew: S.str }
    }
    this.check(BadExample, /field name is reserved/)

    class BadModel2 extends db.Model {
      static SORT_KEY = { getField: S.str }
    }
    this.check(BadModel2, /shadows a property name/)
  }

  testIDName () {
    class SortKeyMustProvideNamesExample extends db.Model {
      static SORT_KEY = S.double
    }
    this.check(SortKeyMustProvideNamesExample, /must define key component name/)

    class PartitionKeyMustProvideNamesExample extends db.Model {
      static KEY = S.double
    }
    this.check(PartitionKeyMustProvideNamesExample, /must define key component name/)

    class OkExample extends db.Model {
      static KEY = { id: S.str }
      static SORT_KEY = null
    }
    OkExample.__doOneTimeModelPrep()

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

  testIndexFieldName () {
    class BadExample extends db.Model {
      static KEY = { name: S.str }
      static SORT_KEY = { rank: S.int }
      static FIELDS = { objField: S.obj() }
      static INDEXES = { badIndex: {} }
    }
    this.check(BadExample, 'partition key is required')

    BadExample.INDEXES = { badIndex: { KEY: ['name'], SORT_KEY: ['dummy', 'missing'] } }
    delete BadExample.__setupDone
    this.check(BadExample, 'all field names must exist in the table')

    BadExample.INDEXES = { badIndex: { KEY: ['missing'], SORT_KEY: ['name'] } }
    delete BadExample.__setupDone
    this.check(BadExample, 'all field names must exist in the table')

    BadExample.INDEXES = { badIndex: { KEY: ['name'], SORT_KEY: ['name'] } }
    delete BadExample.__setupDone
    this.check(BadExample, 'field name cannot be used more than once')

    BadExample.INDEXES = { badIndex: { KEY: ['name'], INCLUDE_ONLY: ['invalid'] } }
    delete BadExample.__setupDone
    this.check(BadExample, 'Field invalid doesn\'t exist in the model')

    BadExample.INDEXES = { badIndex: { KEY: ['name'], INCLUDE_ONLY: ['rank'] } }
    delete BadExample.__setupDone
    this.check(BadExample, 'Field rank is a key attribute and is automatically included')

    BadExample.INDEXES = { badIndex: { KEY: ['objField'], INCLUDE_ONLY: ['name'] } }
    delete BadExample.__setupDone
    this.check(BadExample, 'Field name is a key attribute and is automatically included')
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

class SimpleExample extends db.Model {}

class SimpleExampleTest extends BaseTest {
  async beforeAll () {
    // Create new table should work
    await SimpleExample.createResources()
  }

  testInvalidSetup () {
    function check (badSrc) {
      expect(() => {
        return new SimpleExample(badSrc, true, { id: uuidv4() })
      }).toThrow(/invalid item source type/)
    }
    check('nope')
    check({ isCreate: true })
  }

  testNamingConvention () {
    expect(() => {
      class SomeModel extends db.Model {}
      SomeModel.resourceDefinitions // eslint-disable-line
    }).toThrow(/not include "Model"/)
    expect(() => {
      class SomeTable extends db.Model {}
      SomeTable.resourceDefinitions // eslint-disable-line
    }).toThrow(/not include "Table"/)
  }

  async testFieldNotExtendable () {
    await expect(db.Transaction.run(tx => {
      const row = tx.create(SimpleExample, { id: uuidv4() })
      row.id.weCannotAddPropertiesToFieldsOnModels = undefined
    })).rejects.toThrow(TypeError)
  }

  async testRecreatingTable () {
    // Re-creating the same table shouldn't error out
    await SimpleExample.createResources()
  }

  async testCreateIndex () {
    // GuildMetadataStart
    const GuildMetadata = class extends db.Model {
      static KEY = { name: S.str }
      static FIELDS = { league: S.str, rank: S.int }
      static INDEXES = {
        guildByLeague: { KEY: ['league'], SORT_KEY: ['rank'] },
        guildByRank: { KEY: ['rank'], SORT_KEY: ['league'] }
      }
    }
    // GuildMetadataEnd
    const setupDB = require('../../src/dynamodb/src/dynamodb')
    const dbParams = {
      dynamoDBClient: db.Model.dbClient,
      dynamoDBDocumentClient: db.Model.documentClient,
      autoscalingClient: undefined
    }
    const onDemandDB = setupDB(dbParams)

    await GuildMetadata.createResources()
    const tableDescription = await onDemandDB.Model.dbClient
      .describeTable({ TableName: GuildMetadata.fullTableName })
      .promise()
    expect(tableDescription.Table.GlobalSecondaryIndexes.length).toBe(2)

    let updateParams = {}
    const originalUpdateTableFn = dbParams.dynamoDBClient.updateTable
    const mock = jest.fn().mockImplementation((params) => { updateParams = params })
    dbParams.dynamoDBClient.updateTable = mock

    async function resetAndCreateTable () {
      delete GuildMetadata.__setupDone
      delete GuildMetadata.__createdResource
      delete GuildMetadata.__CACHED_KEY_ORDER
      delete GuildMetadata.__CACHED_SCHEMA
      await GuildMetadata.createResources()
    }

    GuildMetadata.INDEXES = { guildByRank: { KEY: ['rank'] } }
    await resetAndCreateTable()
    expect(updateParams.GlobalSecondaryIndexUpdates[0].Delete.IndexName).toBe('guildByLeague')

    GuildMetadata.INDEXES = {
      guildByLeague: { KEY: ['league'], SORT_KEY: ['rank'] },
      guildByRank: { KEY: ['rank'] },
      anotherIndex: { KEY: ['league', 'rank'] }
    }
    await resetAndCreateTable()
    expect(updateParams.GlobalSecondaryIndexUpdates[0].Create.IndexName).toBe('anotherIndex')
    delete GuildMetadata.INDEXES.anotherIndex

    GuildMetadata.FIELDS.rank = S.int.optional()
    expect(resetAndCreateTable()).rejects.toThrow(/Can not use optional fields as key/)

    GuildMetadata.INDEXES.guildByLeague.SPARSE = true
    GuildMetadata.INDEXES.guildByRank.SPARSE = true

    resetAndCreateTable()

    GuildMetadata.FIELDS.rank = S.int
    GuildMetadata.INDEXES = {
      guildByRank2: { KEY: ['rank'] }
    }
    expect(resetAndCreateTable()).rejects.toThrow(AWSError)
    dbParams.dynamoDBClient.updateTable = originalUpdateTableFn
  }

  async testIndexKeyResourceGeneration () {
    const IndexDBExample = class extends db.Model {
      static KEY = { name: S.str }
      static SORT_KEY = { rank: S.int, score: S.int }
      static FIELDS = { guild: S.str, bool: S.bool, arrField: S.arr(S.obj({ a: S.int })) }
      static INDEXES = {
        index1: { KEY: ['name', 'guild'], SORT_KEY: ['rank', 'score'] },
        index2: { KEY: ['rank'], SORT_KEY: ['name', 'guild'] },
        index3: { KEY: ['name'], SORT_KEY: ['guild'] },
        index4: { KEY: ['arrField'], INCLUDE_ONLY: ['guild'] },
        index5: { KEY: ['bool'] }
      }
    }

    const definitions = IndexDBExample.resourceDefinitions
    const tableParams = Object.values(definitions)
      .filter(val => val.Type === 'AWS::DynamoDB::Table')[0]
      .Properties

    const expectedAttr = ['_id', '_sk', '_c_rank', '_c_guild_name', '_c_arrField', 'guild', '_c_bool']
    const actualAttr = tableParams.AttributeDefinitions.map(attr => attr.AttributeName).sort()
    expect(actualAttr).toEqual(expectedAttr.sort())
    expect(tableParams.GlobalSecondaryIndexes.length).toBe(5)

    expect(tableParams.GlobalSecondaryIndexes[3].Projection.ProjectionType).toBe('INCLUDE')
    expect(tableParams.GlobalSecondaryIndexes[3].Projection.NonKeyAttributes).toEqual(['guild'])
  }

  async testUpdateBillingMode () {
    const setupDB = require('../../src/dynamodb/src/dynamodb')
    const dbParams = {
      dynamoDBClient: db.Model.dbClient,
      dynamoDBDocumentClient: db.Model.documentClient,
      autoscalingClient: undefined
    }
    const onDemandDB = setupDB(dbParams)
    let CapacityExample = class extends onDemandDB.Model {}
    await CapacityExample.createResources()
    let tableDescription = await onDemandDB.Model.dbClient
      .describeTable({ TableName: CapacityExample.fullTableName })
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
    CapacityExample = class extends provisionedDB.Model {}
    await CapacityExample.createResources()
    tableDescription = await provisionedDB.Model.dbClient
      .describeTable({ TableName: CapacityExample.fullTableName })
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
    expect(tempDB.Model.createResources).toBeDefined()
    expect(tempDB.Model.__private).toBe(undefined)
    process.env.INDEBUGGER = oldVal
    jest.resetModules()
  }

  async testWriteModel () {
    const name = uuidv4()
    await txCreate(SimpleExample, { id: name })
    expect((await txGet(SimpleExample, name)).id).toBe(name)
  }

  async testNoExtension () {
    const model = await txGet(SimpleExample, uuidv4())
    expect(() => {
      model.someProp = 1
    }).toThrow()
  }

  async testIdImmutable () {
    const model = await txGet(SimpleExample, uuidv4())
    expect(() => {
      model.id = 'someThingElse'
    }).toThrow()
  }

  async testEventualConsistentGetParams () {
    const getParams = SimpleExample.__getParams(
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
      await tx.get(SimpleExample, uuidv4(), getParams)
    })
    await expect(fut).rejects.toThrow(Error)
    db.Model.__getParams = originalFunc
  }

  async testDescribeTable () {
    const data = await SimpleExample.describeTable()
    expect(data.itemCount).toBeDefined()
    expect(data.sizeInBytes).toBeDefined()
  }
}

class NewModelTest extends BaseTest {
  async testCreateModelIsNew () {
    const result = await db.Transaction.run(tx => {
      const id = uuidv4()
      const model = tx.create(SimpleExample, { id })
      expect(model.id).toBe(id)
      expect(model.id).toBe(SimpleExample.__encodeCompoundValue(
        SimpleExample.__keyOrder.partition, { id }))
      expect(model.isNew).toBe(true)
      tx.__reset() // Don't write anything, cause it will fail.
      return 321
    })
    expect(result).toBe(321) // Make sure it's done
  }

  async testGetNewModel () {
    let ret = await db.Transaction.run(async tx => {
      return tx.get(SimpleExample, uuidv4())
    })
    expect(ret).toBe(undefined)

    ret = await db.Transaction.run(async tx => {
      return tx.get(SimpleExample, uuidv4(), { createIfMissing: true })
    })
    expect(ret).not.toBe(undefined)
  }

  async testNewModelWriteCondition () {
    const id = uuidv4()
    await txCreate(SimpleExample, { id })
    await expect(txCreate(SimpleExample, { id }))
      .rejects.toThrow(
        `Tried to recreate an existing model: SimpleExample _id=${id}`)
  }

  async testNewModelParamsDeprecated () {
    const id = uuidv4()
    const model = await txCreate(SimpleExample, { id })
    expect(model.id).toBe(id)
    expect(model.params).toStrictEqual(undefined)
  }
}

class IDWithSchemaExample extends db.Model {
  static KEY = {
    id: S.str.pattern(/^xyz.*$/).desc(
      'any string that starts with the prefix "xyz"')
  }
}

class CompoundIDExample extends db.Model {
  static KEY = {
    year: S.int.min(1900),
    make: S.str.min(3),
    upc: S.str
  }
}

class ObjKeyExample extends db.Model {
  static KEY = {
    obj: S.obj({
      innerInt: S.int,
      innerStr: S.str
    })
  }
}

class IntKeyExample extends db.Model {
  static KEY = {
    id: S.int
  }
}

class IDSchemaTest extends BaseTest {
  async beforeAll () {
    await IDWithSchemaExample.createResources()
    await CompoundIDExample.createResources()
    await ObjKeyExample.createResources()
    await IntKeyExample.createResources()
  }

  async testSimpleIDWithSchema () {
    const cls = IDWithSchemaExample
    const id = 'xyz' + uuidv4()
    const m1 = await txCreate(cls, { id })
    expect(m1.id).toBe(id)
    await expect(txCreate(cls, { id: 'bad' })).rejects.toThrow(
      S.ValidationError)

    // IDs are checked when keys are created too
    expect(() => cls.key('bad')).toThrow(S.ValidationError)
    const keyOrder = cls.__keyOrder.partition
    expect(() => cls.__encodeCompoundValue(keyOrder, { id: 'X' }))
      .toThrow(S.ValidationError)
    expect(cls.key('xyz').encodedKeys).toEqual({ _id: 'xyz' })
    expect(cls.__encodeCompoundValue(keyOrder, { id: 'xyz' }))
      .toEqual('xyz')
  }

  async testCompoundID () {
    const compoundID = { year: 1900, make: 'Honda', upc: uuidv4() }
    const keyOrder = CompoundIDExample.__keyOrder.partition
    const id = CompoundIDExample.__encodeCompoundValue(
      keyOrder, compoundID)
    function check (entity) {
      expect(entity._id).toBe(id)
      expect(entity.year).toBe(1900)
      expect(entity.make).toBe('Honda')
      expect(entity.upc).toBe(compoundID.upc)
    }

    check(await txCreate(CompoundIDExample, compoundID))
    check(await txGetByKey(CompoundIDExample.data(compoundID)))
    check(await txGet(CompoundIDExample, compoundID))

    expect(() => CompoundIDExample.key({})).toThrow(db.InvalidFieldError)
    expect(() => CompoundIDExample.key({
      year: undefined, // not allowed!
      make: 'Toyota',
      upc: 'nope'
    })).toThrow(db.InvalidFieldError)
    expect(() => CompoundIDExample.key({
      year: 2020,
      make: 'Toy\0ta', // no null bytes!
      upc: 'nope'
    })).toThrow(db.InvalidFieldError)
    expect(() => CompoundIDExample.key({
      year: 2040,
      make: 'need upc too'
    })).toThrow(db.InvalidFieldError)

    const msg = /incorrect number of components/
    expect(() => CompoundIDExample.__decodeCompoundValue(
      keyOrder, '', 'fake')).toThrow(msg)
    expect(() => CompoundIDExample.__decodeCompoundValue(
      keyOrder, id + '\0', 'fake')).toThrow(msg)
    expect(() => CompoundIDExample.__decodeCompoundValue(
      keyOrder, '\0' + id, 'fake')).toThrow(msg)

    expect(() => CompoundIDExample.key('unexpected value')).toThrow(
      db.InvalidParameterError)
  }

  async testObjKeyStableEncoding () {
    const keyOrder = ObjKeyExample.__keyOrder.partition
    const key1 = ObjKeyExample.__encodeCompoundValue(keyOrder,
      {
        obj: {
          innerInt: 10,
          innerStr: 'xyz'
        }
      }
    )
    const key2 = ObjKeyExample.__encodeCompoundValue(keyOrder,
      {
        obj: {
          innerStr: 'xyz',
          innerInt: 10
        }
      }
    )
    expect(key1).toStrictEqual(key2)
  }

  async testIntKey () {
    const key = IntKeyExample.__encodeCompoundValue(
      IntKeyExample.__keyOrder.partition, { id: 2342 }, true)
    expect(key).toBe(2342)

    const decoded = IntKeyExample.__decodeCompoundValue(
      IntKeyExample.__keyOrder.partition, 2, '_sk', true)
    expect(decoded).toEqual({ id: 2 })
  }
}

class BasicExample extends db.Model {
  static FIELDS = {
    noRequiredNoDefault: S.double.optional()
  }
}

class WriteTest extends BaseTest {
  async beforeAll () {
    await BasicExample.createResources()
    this.modelName = uuidv4()
    await txGet(BasicExample, this.modelName, model => {
      model.noRequiredNoDefault = 0
    })
  }

  async testNoIDInUpdateCondition () {
    const m1 = await txGet(BasicExample, this.modelName)
    const params = m1.__updateParams()
    if (params[CONDITION_EXPRESSION_STR]) {
      expect(params[CONDITION_EXPRESSION_STR]).not.toContain('id=')
    }
  }

  async testNoIdInPutCondition () {
    await txGet(BasicExample, this.modelName, model => {
      const params = model.__putParams()
      if (params.ConditionExpression) {
        expect(params.ConditionExpression).not.toContain('id=')
      }
    })
  }

  async testAttributeEncoding () {
    await txGet(BasicExample, this.modelName, model => {
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
    const m1 = await txGet(BasicExample, this.modelName)
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
    const model = await txGet(BasicExample, uuidv4())
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
    let model = await txGet(BasicExample, uuidv4(), model => {
      expect(model.isNew).toBe(true)
      expect(model.noRequiredNoDefault).toBe(undefined)
      model.noRequiredNoDefault = 1
    })

    // Reset the prop to undefined should delete it
    model = await txGet(BasicExample, model.id, model => {
      expect(model.noRequiredNoDefault).toBe(1)
      model.noRequiredNoDefault = undefined

      const propName = 'noRequiredNoDefault'
      expect(model).toHaveProperty(propName)
      expect(model.__putParams().Item).not.toHaveProperty(propName)
      expect(model.__updateParams()[UPDATE_EXPRESSION_STR])
        .toContain('REMOVE ' + model.getField(propName).__awsName)
    })

    // Read and check again
    model = await txGet(BasicExample, model.id)
    expect(model.noRequiredNoDefault).toBe(undefined)
  }

  async testNoLockOption () {
    const model = await txGet(BasicExample, this.modelName)
    model.getField('noRequiredNoDefault').incrementBy(1)
    expect(model.__updateParams().ExpressionAttributeNames)
      .not.toContain('noRequiredNoDefault')
  }

  async testPutNoLock () {
    const model = await txGet(BasicExample, this.modelName)
    model.getField('noRequiredNoDefault').incrementBy(1)
    expect(model.__putParams().ExpressionAttributeNames)
      .not.toContain('noRequiredNoDefault')
  }

  async testRetry () {
    const model = await txGet(BasicExample, this.modelName)
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
    await BasicExample.createResources()
    this.modelName = uuidv4()
    await txGet(BasicExample, this.modelName)
  }

  async testNewModel () {
    const m1 = await txGet(BasicExample, uuidv4())
    expect(m1.isNew).toBe(true)
    expect(m1.__isMutated()).toBe(true)
  }

  async testMutatedModel () {
    const m1 = await txGet(BasicExample, this.modelName)
    expect(m1.__isMutated()).toBe(false)
    m1.noRequiredNoDefault += 1
    expect(m1.__isMutated()).toBe(true)
  }

  async testConditionCheckMutatedModel () {
    const m1 = await txGet(BasicExample, this.modelName)
    m1.noRequiredNoDefault += 1
    expect(() => {
      m1.__conditionCheckParams()
    }).toThrow()
  }

  async testConditionCheckUnchangedModel () {
    const m1 = await txGet(BasicExample, this.modelName)
    expect(m1.__conditionCheckParams().ConditionExpression)
      .toBe('attribute_exists(#_id)')
  }

  async testReadonlyModel () {
    const m1 = await txGet(BasicExample, this.modelName)
    m1.noRequiredNoDefault // eslint-disable-line no-unused-expressions
    const awsName = m1.getField('noRequiredNoDefault').__awsName
    expect(m1.__conditionCheckParams()).toHaveProperty('ConditionExpression',
      `attribute_exists(#_id) AND attribute_not_exists(${awsName})`)
  }
}

class RangeKeyExample extends db.Model {
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
      SimpleExample.createResources(),
      RangeKeyExample.createResources()
    ])
  }

  async testGetNoCreateIfMissingWithExcessFields () {
    const fut = db.Transaction.run(async tx => {
      // can't specify field like "n" when reading unless we're doing a
      // createIfMissing=true
      await tx.get(RangeKeyExample, { id: uuidv4(), rangeKey: 3, n: 3 })
    })
    await expect(fut).rejects.toThrow(/received non-key fields/)
  }

  testDataKey () {
    const id = uuidv4()
    const data = RangeKeyExample.data({ id, rangeKey: 1, n: 5 })
    const key = data.key
    expect(key.keyComponents.id).toBe(id)
    expect(key.keyComponents.rangeKey).toBe(1)
    expect(data.data.n).toBe(5)
  }

  async testGetWithWrongType () {
    await expect(db.Transaction.run(async tx => {
      await tx.get(RangeKeyExample.key({ id: uuidv4(), rangeKey: 2 }), {
        createIfMissing: true
      })
    })).rejects.toThrow(/must pass a Data/)

    await expect(db.Transaction.run(async tx => {
      await tx.get(RangeKeyExample.data({ id: uuidv4(), rangeKey: 2, n: 3 }))
    })).rejects.toThrow(/must pass a Key/)
  }

  async testSortKey () {
    async function check (id, rangeKey, n, create = true) {
      const encodedKeys = { id, rangeKey }
      if (create) {
        await txCreate(RangeKeyExample, { ...encodedKeys, n })
      }
      const model = await txGet(RangeKeyExample, encodedKeys)
      expect(model.id).toBe(id)
      expect(model.rangeKey).toBe(rangeKey)
      expect(model._sk).toBe(rangeKey)
      expect(model.n).toBe(n)
    }

    const id1 = uuidv4()
    await check(id1, 1, 0)

    // changing the sort key means we're working with a different row
    await check(id1, 2, 1)
    await check(id1, 1, 0, false)

    // changing the partition key but not the sort key also means we're working
    // with a different row
    const id2 = uuidv4()
    await check(id2, 1, 2)
    await check(id2, 2, 3)
    await check(id1, 1, 0, false)
    await check(id1, 2, 1, false)

    // should be able to update fields in a model with a sort key
    await db.Transaction.run(async tx => {
      await tx.update(RangeKeyExample, { id: id1, rangeKey: 1, n: 0 }, { n: 99 })
    })
    await check(id1, 1, 99, false)
    // but not the sort key itself
    // this throws because no such row exists:
    await expect(db.Transaction.run(async tx => {
      await tx.update(RangeKeyExample, { id: id1, rangeKey: 9, n: 0 }, { n: 99 })
    })).rejects.toThrow()
    // these last two both throw because we can't modify key values
    await expect(db.Transaction.run(async tx => {
      const x = await tx.get(RangeKeyExample, { id: id1, rangeKey: 1 })
      x.rangeKey = 2
    })).rejects.toThrow(/rangeKey is immutable/)
    await expect(db.Transaction.run(async tx => {
      await tx.update(RangeKeyExample, { id: id1, rangeKey: 1 }, { rangeKey: 2 })
    })).rejects.toThrow(/must not contain key fields/)
    await expect(db.Transaction.run(async tx => {
      const x = await tx.get(RangeKeyExample, { id: id1, rangeKey: 1 })
      x.id = uuidv4()
    })).rejects.toThrow(/id is immutable/)
    await expect(db.Transaction.run(async tx => {
      await tx.update(RangeKeyExample, { id: id1, rangeKey: 1 }, { id: id2 })
    })).rejects.toThrow(/must not contain key fields/)
  }

  async testValidKey () {
    SimpleExample.key(uuidv4())
    SimpleExample.key({ id: uuidv4() })
    RangeKeyExample.key({ id: uuidv4(), rangeKey: 1 })
  }

  async testInvalidKey () {
    const id = uuidv4()
    const invalidIDsForSimpleExample = [
      // these aren't even valid IDs
      1,
      '',
      String(''),
      undefined,
      {},
      [],
      { id, abc: 123 }
    ]
    for (const keyValues of invalidIDsForSimpleExample) {
      expect(() => {
        SimpleExample.key(keyValues)
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
        RangeKeyExample.key(keyValues)
      }).toThrow()
    }
  }

  testDeprecatingLegacySyntax () {
    expect(() => {
      SimpleExample.key('id', 123)
    }).toThrow()
  }
}

class PXPayout extends db.Model {
  static KEY = { player: S.str, admin: S.str }
  static FIELDS = {
    payout: S.int, date: S.str.optional(), notes: S.str.optional(), status: S.bool
  }

  static INDEXES = {
    payoutByPlayer: { KEY: ['player'], SORT_KEY: ['admin', 'payout'] },
    payoutByAdmin: { KEY: ['admin'], SORT_KEY: ['payout'] },
    payoutByStatus: { KEY: ['status'], INCLUDE_ONLY: ['date'] }
  }
}

class IndexTest extends BaseTest {
  async beforeAll () {
    await PXPayout.createResources()
  }

  async testIndexFieldGeneration () {
    function validate (model, fields, val) {
      const fieldName = PXPayout.__encodeCompoundFieldName(fields)
      expect(model.__cached_attrs[fieldName].get()).toBe(val)
    }
    const name = uuidv4()
    const model1 = await txCreate(PXPayout, { player: name, admin: 'b', payout: 0, status: true })
    validate(model1, PXPayout.INDEXES.payoutByPlayer.KEY, name)
    validate(model1, PXPayout.INDEXES.payoutByPlayer.SORT_KEY, ['b', 0].join('\0'))
    validate(model1, PXPayout.INDEXES.payoutByAdmin.KEY, 'b')
    validate(model1, PXPayout.INDEXES.payoutByStatus.KEY, 'true')
  }

  async testEditIndexField () {
    const name = uuidv4()
    const model1 = await txCreate(PXPayout, { player: name, admin: 'b', payout: 0, status: true })
    expect(() => { model1._c_admin_payout = 'xyz' }).toThrow(db.InvalidFieldError)
  }
}

class JSONExample extends db.Model {
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

class JSONExampleTest extends BaseTest {
  async beforeAll () {
    await JSONExample.createResources()
  }

  async testRequiredFields () {
    const obj = { ab: 2 }
    const arr = [{ cd: 2 }, { cd: 1 }]
    async function check (input) {
      input.id = uuidv4()
      await expect(txGet(JSONExample, input)).rejects.toThrow(
        /missing required value/)
    }
    await check({})
    await check({ objNoDefaultRequired: obj })
    await check({ arrNoDefaultRequired: arr })

    const id = uuidv4()
    async function checkOk (input) {
      const model = await txGet(JSONExample, input)
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
    await txGet(JSONExample, data, model => {
      expect(model.isNew).toBe(true)
    })

    await txGet(JSONExample, id, model => {
      obj.cd.push(1)
      model.objNoDefaultRequired.cd.push(1)
      arr[0].bc = 32
      model.arrNoDefaultRequired[0].bc = 32
    })

    await txGet(JSONExample, id, model => {
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
      return tx.get(JSONExample, data, { createIfMissing: true })
    })
    expect(model.toJSON()).toEqual(data)
  }
}

class GetArgsParserTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await SimpleExample.createResources()
  }

  async testJustAModel () {
    await expect(db.__private.getWithArgs([SimpleExample], () => {})).rejects
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
    const params = [SimpleExample]
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
    const params = [SimpleExample.key(uuidv4())]
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
    keys.push(SimpleExample.key(id1), SimpleExample.key(id2))
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
    await BasicExample.createResources()
    this.modelNames = [uuidv4(), uuidv4()]
    const promises = this.modelNames.map(name => {
      return txGet(BasicExample, name, (m) => {
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
    const model = await txGet(BasicExample, uuidv4())
    expect(() => batcher.__write(model)).toThrow()
  }

  async testDupWrite () {
    const batcher = new db.__private.__WriteBatcher()
    const model = await txGet(BasicExample, uuidv4())
    batcher.track(model)
    model.noRequiredNoDefault += 1
    batcher.__write(model)
    expect(() => batcher.__write(model)).toThrow()
  }

  async testReadonly () {
    const batcher = new db.__private.__WriteBatcher()
    const model1 = await txGet(BasicExample, this.modelNames[0])
    const model2 = await txGet(BasicExample, this.modelNames[1])
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
    await ReservedAttrName.createResources()
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
      const row = tx.create(IDWithSchemaExample, { id: 'xyz' + uuidv4() })
      itemSourceCreate = row.__src
    })

    const batcher = new db.__private.__WriteBatcher()
    batcher.track({
      __fullTableName: 'sharedlibTestData',
      tableName: 'TestData',
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

    const row = { _id: { S: '123' } }
    reasons.push({
      Code: 'ConditionalCheckFailed',
      Item: row
    })
    const request = {
      params: {
        TransactItems: [{
          Put: {
            Item: row,
            TableName: 'sharedlibTestData'
          }
        }]
      }
    }
    response.error = undefined
    batcher.__extractError(request, response)
    expect(response.error.message)
      .toBe('Tried to recreate an existing model: sharedlibTestData _id=123')

    batcher.__allModels[0]._sk = '456'
    request.params.TransactItems = [
      {
        Update: {
          Key: { _id: { S: '123' }, _sk: { S: '456' } },
          TableName: 'sharedlibTestData'
        }
      }
    ]
    response.error = undefined
    batcher.__extractError(request, response)
    expect(response.error.message)
      .toBe([
        'Tried to recreate an existing model: ',
        'sharedlibTestData _id=123 _sk=456'].join(''))

    response.error = undefined
    batcher.__allModels[0].__src = 'something else'
    batcher.__extractError(request, response)
    expect(response.error).toBe(undefined)

    reasons[0].Code = 'anything else'
    response.error = undefined
    batcher.__extractError({}, response)
    expect(response.error).toBe(undefined)
  }

  async TestDataAlreadyExistsError () {
    // Single row transactions
    const id = uuidv4()
    await txCreate(BasicExample, { id })
    let fut = txCreate(BasicExample, { id })
    await expect(fut).rejects.toThrow(db.ModelAlreadyExistsError)

    // Multi-row transactions
    fut = db.Transaction.run(async (tx) => {
      tx.create(BasicExample, { id })
      tx.create(BasicExample, { id: uuidv4() })
    })
    await expect(fut).rejects.toThrow(db.ModelAlreadyExistsError)
  }

  async testInvalidModelUpdateError () {
    const id = uuidv4()
    let fut = db.Transaction.run(async (tx) => {
      tx.update(BasicExample, { id }, { noRequiredNoDefault: 1 })
    })
    await expect(fut).rejects.toThrow(db.InvalidModelUpdateError)

    fut = db.Transaction.run(async (tx) => {
      tx.create(BasicExample, { id: uuidv4() })
      tx.update(BasicExample, { id }, { noRequiredNoDefault: 1 })
    })
    await expect(fut).rejects.toThrow(db.InvalidModelUpdateError)
  }

  /**
   * Verify creating a model with invalid key fails
   */
  async testInvalidKey () {
    let createPromise = db.Transaction.run(async tx => {
      tx.create(BasicExample, { id: { test: 'not valid schema' } })
    })
    await expect(createPromise).rejects.toThrow(S.ValidationError)

    createPromise = db.Transaction.run(async tx => {
      return tx.get(BasicExample, { id: { test: 'not valid schema' } }, { createIfMissing: true })
    })

    await expect(createPromise).rejects.toThrow(S.ValidationError)
  }

  /**
   * Verify modifying keyparts is not allowed
   */
  async testMutatingKeyparts () {
    await CompoundIDExample.createResources()
    const compoundID = { year: 1900, make: 'Honda', upc: uuidv4() }
    let createPromise = db.Transaction.run(async tx => {
      const model = tx.create(CompoundIDExample, compoundID)
      model.year = 1901
    })
    await expect(createPromise).rejects.toThrow(db.InvalidFieldError)

    await db.Transaction.run(async tx => {
      return tx.create(CompoundIDExample, compoundID)
    })

    createPromise = db.Transaction.run(async tx => {
      const model = await tx.get(CompoundIDExample, compoundID)
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
    class NestedDefaultsExample extends db.Model {
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
    await NestedDefaultsExample.createResources()
    const id = uuidv4()

    await db.Transaction.run(async tx => {
      tx.create(NestedDefaultsExample, {
        id: id,
        arr: [{ int: 2 }, { int: 3 }]
      })
    })

    await db.Transaction.run(async tx => {
      const result = await tx.get(NestedDefaultsExample, id)
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

    class NestedDefaultsExample extends db.Model {
      static FIELDS = fields
    }

    await NestedDefaultsExample.createResources()
    const id = uuidv4()

    await db.Transaction.run(async tx => {
      tx.create(NestedDefaultsExample, {
        id: id,
        arr: [{ int: 2 }, { int: 3 }]
      })
    })

    fields.arr.itemsSchema.__isLocked = false
    fields.arr.itemsSchema.prop('newField', S.str.default('newDefault'))
    delete NestedDefaultsExample.__setupDone
    NestedDefaultsExample.__doOneTimeModelPrep()

    await db.Transaction.run(async tx => {
      const result = await tx.get(NestedDefaultsExample, id)
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

class OptDefaultExampleTest extends BaseTest {
  async testFieldWhichIsBothOptionalAndDefault () {
    class OptDefaultExample extends db.Model {
      static get FIELDS () {
        return {
          def: S.int.default(7),
          opt: S.int.optional(),
          defOpt: S.int.default(7).optional()
        }
      }
    }
    await OptDefaultExample.createResources()

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
      check(tx.create(OptDefaultExample, { id: idSpecifyNothing }),
        7, undefined, 7)

      // can use our own values (specify all field values)
      check(tx.create(OptDefaultExample, {
        id: idSpecifyAll,
        def: 1,
        opt: 2,
        defOpt: 3
      }), 1, 2, 3)

      // optional fields with a default can still be omitted from the db (i.e.,
      // assigned a value of undefined)
      check(tx.create(OptDefaultExample, {
        id: idUndef,
        defOpt: undefined
      }), 7, undefined, undefined)
    })

    // verify that these are all properly stored to the database
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultExample, idSpecifyNothing), 7, undefined, 7)
      check(await tx.get(OptDefaultExample, idSpecifyAll), 1, 2, 3)
      check(await tx.get(OptDefaultExample, idUndef), 7, undefined, undefined)
    })

    // add a new set of fields (normally we'd do this on the same model, but
    // for the test we do it in a new model (but SAME TABLE) because one-time
    // setup is already done for the other model)
    class OptDefaultExample2 extends db.Model {
      static tableName = OptDefaultExample.name
      static FIELDS = {
        ...OptDefaultExample.FIELDS,
        def2: S.int.default(8),
        opt2: S.int.optional(),
        defOpt2: S.int.default(8).optional()
      }
    }
    await OptDefaultExample2.createResources()

    // the default value for new fields isn't stored in the db yet (old rows
    // have not been changed yet)
    let fut = db.Transaction.run(async tx => {
      await tx.update(OptDefaultExample2,
        { id: idSpecifyNothing, def2: 8 }, { def: 1 })
    })
    await expect(fut).rejects.toThrow(/outdated \/ invalid conditions/)

    // we can (ONLY) use update() on defaults that have been written to the db
    await db.Transaction.run(async tx => {
      await tx.update(OptDefaultExample2,
        { id: idSpecifyNothing, def: 7 }, { opt2: 11 })
    })

    // blind updates are only partial, so they won't populate a new default
    // field unless explicitly given a value for it
    fut = db.Transaction.run(async tx => {
      await tx.update(OptDefaultExample2,
        { id: idSpecifyNothing, def2: 8 }, { def: 2 })
    })
    await expect(fut).rejects.toThrow(/outdated \/ invalid conditions/)

    // verify that these are all in the proper state when accessing old rows;
    // also, accessing the row populates the default value for the new field
    // which triggers a database write!
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultExample2, idSpecifyNothing),
        7, undefined, 7,
        8, 11, undefined)
    })
    await db.Transaction.run(async tx => {
      // verify the db was updated by doing a blind update dependent on it
      await tx.update(OptDefaultExample2,
        { id: idSpecifyNothing, def2: 8 }, { def: 100 })
    })
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultExample2, idSpecifyNothing),
        100, undefined, 7, 8, 11, undefined)
    })

    // accessing and modifying an old row will also write the new defaults to
    // the db
    await db.Transaction.run(async tx => {
      const row = await tx.get(OptDefaultExample2, idUndef)
      check(row, 7, undefined, undefined,
        8, undefined, undefined)
      row.def = 3
    })
    await db.Transaction.run(async tx => {
      // verify the db was updated by doing a blind update dependent on it
      await tx.update(OptDefaultExample2,
        { id: idUndef, def: 3, def2: 8 }, { opt2: 101 })
    })
    await db.Transaction.run(async tx => {
      check(await tx.get(OptDefaultExample2, idUndef),
        3, undefined, undefined, 8, 101, undefined)
    })
  }
}

class OptionalFieldConditionTest extends BaseTest {
  async testOptFieldCondition () {
    class OptNumExample extends db.Model {
      static get FIELDS () {
        return {
          n: S.int.optional()
        }
      }
    }
    await OptNumExample.createResources()

    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(OptNumExample, { id })
    })
    await db.Transaction.run(async tx => {
      const row = await tx.get(OptNumExample, id)
      if (row.n === undefined) {
        row.n = 5
      }
      const field = row.getField('n')
      const [condition, vals] = field.__conditionExpression(':_1')
      expect(condition).toBe(`attribute_not_exists(${field.__awsName})`)
      expect(vals).toEqual({})
    })
  }
}

class TTLExample extends db.Model {
  static FIELDS = {
    expirationTime: S.int,
    doubleTime: S.double,
    notTime: S.str.optional(),
    optionalTime: S.int.optional()
  }

  static EXPIRE_EPOCH_FIELD = 'expirationTime'
}

class NoTTLExample extends TTLExample {
  static EXPIRE_EPOCH_FIELD = undefined
}

class TTLTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await TTLExample.createResources()
    await NoTTLExample.createResources()
  }

  async testTTL () {
    const id = uuidv4()
    const currentTime = Math.floor(new Date().getTime() / 1000)
    await db.Transaction.run(tx => {
      tx.create(TTLExample, {
        id,
        expirationTime: currentTime + 1,
        doubleTime: 1
      })
    })

    await new Promise((resolve, reject) => {
      setTimeout(resolve, 2000)
    })

    const model = await db.Transaction.run(tx => {
      return tx.get(TTLExample, id)
    })
    expect(model).toBeUndefined()
  }

  async testCFResource () {
    expect(Object.values(TTLExample.resourceDefinitions)[0].Properties)
      .toHaveProperty('TimeToLiveSpecification')
  }

  async testConfigValidation () {
    const Cls1 = class extends TTLExample {
      static EXPIRE_EPOCH_FIELD = 'notTime'
    }
    expect(() => {
      Cls1.resourceDefinitions // eslint-disable-line
    }).toThrow('must refer to an integer or double field')

    const Cls2 = class extends TTLExample {
      static EXPIRE_EPOCH_FIELD = 'optionalTime'
    }
    Cls2.resourceDefinitions // eslint-disable-line

    const Cls3 = class extends TTLExample {
      static EXPIRE_EPOCH_FIELD = 'doubleTime'
    }
    expect(() => {
      Cls3.resourceDefinitions // eslint-disable-line
    }).not.toThrow()

    const Cls4 = class extends TTLExample {
      static EXPIRE_EPOCH_FIELD = 'invalid'
    }
    expect(() => {
      Cls4.resourceDefinitions // eslint-disable-line
    }).toThrow('EXPIRE_EPOCH_FIELD must refer to an existing field')
  }

  async testExpiration () {
    const currentTime = Math.ceil(new Date().getTime() / 1000)
    const result = await db.Transaction.run(async tx => {
      const model = tx.create(TTLExample,
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
      const model1 = tx.create(NoTTLExample,
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
      tx.create(NoTTLExample, {
        id,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
    })

    // Turn on ttl locally now
    NoTTLExample.EXPIRE_EPOCH_FIELD = 'expirationTime'

    // if not createIfMissing, nothing should be returned
    let model = await db.Transaction.run(tx => {
      return tx.get(NoTTLExample, id)
    })
    expect(model).toBeUndefined()

    // if createIfMissing, a new model should be returned
    model = await db.Transaction.run(tx => {
      return tx.get(NoTTLExample,
        { id, expirationTime: currentTime + 10000, doubleTime: 111 },
        { createIfMissing: true })
    })
    expect(model.isNew).toBe(true)

    model = await db.Transaction.run(tx => {
      return tx.get(NoTTLExample, id)
    })
    expect(model.doubleTime).toBe(111)
    expect(model.isNew).toBe(false)

    NoTTLExample.EXPIRE_EPOCH_FIELD = undefined
  }

  async testOverrideExpiredModel () {
    // When blind write to a model with TTL enabled, the condition must take
    // expired but not yet deleted models into account, and don't fail the tx
    const currentTime = Math.ceil(new Date().getTime() / 1000)

    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(NoTTLExample, {
        id,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
    })
    // Turn on ttl locally now
    NoTTLExample.EXPIRE_EPOCH_FIELD = 'expirationTime'

    await db.Transaction.run(tx => {
      tx.create(NoTTLExample,
        { id, expirationTime: currentTime + 1000, doubleTime: 111 })
    })

    const model = await db.Transaction.run(tx => {
      return tx.get(NoTTLExample, id)
    })
    expect(model.doubleTime).toBe(111)

    NoTTLExample.EXPIRE_EPOCH_FIELD = undefined
  }

  async testBatchGetExpired () {
    const currentTime = Math.ceil(new Date().getTime() / 1000)

    const id = uuidv4()
    const id2 = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(NoTTLExample, {
        id,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
      tx.create(NoTTLExample, {
        id: id2,
        expirationTime: currentTime - 10000,
        doubleTime: 11223
      })
    })
    // Turn on ttl locally now
    NoTTLExample.EXPIRE_EPOCH_FIELD = 'expirationTime'

    const result = await db.Transaction.run(tx => {
      return tx.get([
        NoTTLExample.key(id), NoTTLExample.key(uuidv4())
      ], { inconsistentRead: false })
    })
    expect(result).toStrictEqual([undefined, undefined])

    const result1 = await db.Transaction.run(tx => {
      return tx.get([
        NoTTLExample.key(id), NoTTLExample.key(uuidv4())
      ], { inconsistentRead: true })
    })
    expect(result1).toStrictEqual([undefined, undefined])

    const result2 = await db.Transaction.run(tx => {
      return tx.get([
        NoTTLExample.data({
          id,
          expirationTime: currentTime - 10000,
          doubleTime: 1
        }),
        NoTTLExample.data({
          id: id2,
          expirationTime: currentTime - 10000,
          doubleTime: 1
        })
      ], { inconsistentRead: false, createIfMissing: true })
    })
    expect(result2.length).toBe(2)
    expect(result2[0].id).toBe(id)
    expect(result2[1].id).toBe(id2)

    NoTTLExample.EXPIRE_EPOCH_FIELD = undefined
  }
}

class SnapshotTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await JSONExample.createResources()
    this.modelID = uuidv4()
    await db.Transaction.run(async tx => {
      await tx.get(JSONExample, {
        id: this.modelID,
        objNoDefaultRequired: { ab: 11 },
        arrNoDefaultRequired: []
      }, { createIfMissing: true })
    })
  }

  async testGetNewModel () {
    const id = uuidv4()
    const result = await db.Transaction.run(async tx => {
      const m = await tx.get(JSONExample,
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
      const m = await tx.get(JSONExample, this.modelID)
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
      const m = await tx.get(RangeKeyExample, { id, rangeKey: 1, n: 1 }, { createIfMissing: true })
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
        _sk: 1,
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
    const keys = new db.UniqueKeyList(NoTTLExample.key(id))
    keys.push(NoTTLExample.key(id), NoTTLExample.key(uuidv4()))
    expect(keys.length).toBe(2)
    keys.push(NoTTLExample.key(id))
    expect(keys.length).toBe(2)
  }

  async testGet () {
    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(SimpleExample, { id })
    })
    const keys = new db.UniqueKeyList(SimpleExample.key(id))
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
  IndexTest,
  JSONExampleTest,
  KeyTest,
  NewModelTest,
  OptDefaultExampleTest,
  OptionalFieldConditionTest,
  SimpleExampleTest,
  SnapshotTest,
  TTLTest,
  WriteBatcherTest,
  WriteTest,
  UniqueKeyListTest
)
