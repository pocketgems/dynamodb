const assert = require('assert')

const uuidv4 = require('uuid').v4

const db = require('../src/dynamodb')
const S = require('../src/schema')

const { BaseTest, runTests } = require('./base-unit-test')

async function txGetGeneric (cls, values, func) {
  return db.Transaction.run(async tx => {
    let model
    const valuesType = values.constructor.name
    if (valuesType === 'Key' || valuesType === 'Data') {
      model = await tx.get(values, { createIfMissing: true })
    } else {
      model = await tx.get(cls, values, { createIfMissing: true })
    }
    if (func) {
      func(model)
    }
    return model
  })
}
async function txGet (keyValues, func) {
  return txGetGeneric(TransactionModel, keyValues, func)
}
async function txGetRequired (keyValues, func) {
  return txGetGeneric(TransactionModelWithRequiredField, keyValues, func)
}

class TransactionModel extends db.Model {
  static KEY = { id: S.str.min(1) }
  static FIELDS = {
    field1: S.double.optional(),
    field2: S.double.optional(),
    arrField: S.arr(S.obj({ a: S.int.optional() })).optional(),
    objField: S.obj({
      a: S.obj({
        a: S.int.optional()
      }).optional()
    }).optional()
  }
}
class TransactionModelWithRequiredField extends TransactionModel {
  static FIELDS = { ...super.FIELDS, required: S.double }
}

class QuickTransactionTest extends BaseTest {
  mockTransactionDefaultOptions (options) {
    Object.defineProperty(db.Transaction.prototype, 'defaultOptions', {
      value: options,
      writable: false
    })
  }

  async beforeAll () {
    await super.beforeAll()
    await TransactionModel.createUnittestResource()
    await TransactionModelWithRequiredField.createUnittestResource()
    this.oldTransactionOptions = db.Transaction.prototype.defaultOptions
    const newOptions = Object.assign({}, this.oldTransactionOptions)
    Object.assign(newOptions, { retries: 1, initialBackoff: 20 })
    this.mockTransactionDefaultOptions(newOptions)
  }

  async afterAll () {
    super.afterAll()
    this.mockTransactionDefaultOptions(this.oldTransactionOptions)
  }
}

class ParameterTest extends BaseTest {
  testGoodOptions () {
    const badOptions = [
      { retries: 0 },
      { initialBackoff: 1 },
      { maxBackoff: 200 }
    ]
    for (const opt of badOptions) {
      expect(() => {
        new db.Transaction(opt) // eslint-disable-line no-new
      }).not.toThrow()
    }
  }

  testBadOptions () {
    const badOptions = [
      { retries: -1 },
      { initialBackoff: 0 },
      { maxBackoff: 199 },
      { notAValidOption: 1 },
      { retries: 'wrong type' }
    ]
    for (const opt of badOptions) {
      expect(() => {
        new db.Transaction(opt) // eslint-disable-line no-new
      }).toThrow(db.InvalidOptionsError)
    }
  }

  async testBadRunParam () {
    await expect(db.Transaction.run(1, 2)).rejects
      .toThrow(db.InvalidParameterError)

    await expect(db.Transaction.run({}, 2)).rejects
      .toThrow(db.InvalidParameterError)

    await expect(db.Transaction.run(1, () => {})).rejects
      .toThrow(db.InvalidParameterError)

    await expect(db.Transaction.run(1, 2, 3)).rejects
      .toThrow(db.InvalidParameterError)
  }
}

class KeyOnlyModel extends db.Model {
  static KEY = { id: S.str.min(1) }
  static SORT_KEY = { sk: S.str.min(1) }
}

class KeyOnlyModel2 extends KeyOnlyModel {
  static tableName = KeyOnlyModel.tableName // same
}

class TransactionEdgeCaseTest extends BaseTest {
  async beforeAll () {
    await KeyOnlyModel.createUnittestResource()
    await KeyOnlyModel2.createUnittestResource()
  }

  async afterEach () {
    jest.restoreAllMocks()
  }

  async testKeyCollision () {
    const suffix = uuidv4()
    await db.Transaction.run(tx => {
      const i1 = tx.create(KeyOnlyModel, { id: 'x', sk: 'y' + suffix })
      const i2 = tx.create(KeyOnlyModel, { id: 'xy', sk: suffix })
      expect(i1.toString()).not.toEqual(i2.toString())
    })
  }

  async testKeyCollisionFromSeparateModels () {
    const suffix = uuidv4()
    let checked = false
    const promise = db.Transaction.run(async tx => {
      const i1 = tx.create(KeyOnlyModel, { id: 'x', sk: suffix })
      await db.Transaction.run(tx => {
        const i2 = tx.create(KeyOnlyModel2, { id: 'x', sk: suffix })
        expect(i1.toString()).toEqual(i2.toString())
        checked = true
      })
    })
    await expect(promise).rejects.toThrow(db.ModelAlreadyExistsError)
    expect(checked).toBe(true)
  }

  async testConditionedOnNonExistentItem () {
    const idToRead = { id: uuidv4(), sk: 'x' }
    const idToWrite = { id: uuidv4(), sk: 'y' }

    // make transactWrite() is called with the proper parameters
    const __WriteBatcher = db.__private.__WriteBatcher
    const spy = jest.spyOn(__WriteBatcher.prototype, 'transactWrite')
    await db.Transaction.run(async tx => {
      const item = await tx.get(KeyOnlyModel, idToRead)
      if (!item) {
        tx.create(KeyOnlyModel, idToWrite)
      }
    })
    await db.Transaction.run(async tx => {
      expect(await tx.get(KeyOnlyModel, idToRead)).toBe(undefined)
      expect(await tx.get(KeyOnlyModel, idToWrite)).not.toBe(undefined)
    })
    expect(spy).toHaveBeenCalledTimes(1)
    const callArgs = spy.mock.calls[0]
    expect(callArgs.length).toBe(1)
    expect(Object.keys(callArgs[0])).toEqual(['TransactItems'])
    const txItems = callArgs[0].TransactItems
    expect(txItems.length).toBe(2)
    const putIdx = txItems[0].Put ? 0 : 1
    const putExpr = txItems[putIdx]
    const checkExpr = txItems[1 - putIdx]
    expect(putExpr).toEqual({
      Put: {
        TableName: 'sharedlibKeyOnlyModel',
        Item: {
          _id: idToWrite.id,
          _sk: 'y'
        },
        ConditionExpression: 'attribute_not_exists(#0)',
        ExpressionAttributeNames: { '#0': '_id' }
      }
    })
    expect(checkExpr).toEqual({
      ConditionCheck: {
        TableName: 'sharedlibKeyOnlyModel',
        Key: {
          _id: idToRead.id,
          _sk: 'x'
        },
        ConditionExpression: 'attribute_not_exists(#0)',
        ExpressionAttributeNames: { '#0': '_id' }
      }
    })
  }
}

class TransactionGetTest extends QuickTransactionTest {
  async beforeAll () {
    await super.beforeAll()
    this.modelName = uuidv4()
    await txGet(this.modelName)
  }

  async testGetItemTwice () {
    await db.Transaction.run(async (tx) => {
      await tx.get(TransactionModel, 'a',
        { createIfMissing: true })
      const fut = tx.get(TransactionModel, 'a',
        { createIfMissing: true })
      await expect(fut).rejects.toThrow()
    })
  }

  async testGetModelByID () {
    await db.Transaction.run(async (tx) => {
      const model = await tx.get(TransactionModel, 'a',
        { createIfMissing: true })
      expect(model.id).toBe('a')
    })
  }

  async testGetModelByKey () {
    await db.Transaction.run(async (tx) => {
      const model = await tx.get(TransactionModel.data('a'),
        { createIfMissing: true })
      expect(model.id).toBe('a')
    })
  }

  async testGetModelByKeys () {
    await db.Transaction.run(async (tx) => {
      const [m1, m2] = await tx.get([
        TransactionModel.data('a'),
        TransactionModel.data('b')
      ], { createIfMissing: true })
      expect(m1.id).toBe('a')
      expect(m2.id).toBe('b')
    })
  }

  async _testMultipleGet (inconsistentRead) {
    const newName = uuidv4()
    const [m1, m2] = await db.Transaction.run(async (tx) => {
      return tx.get([
        TransactionModel.key(this.modelName),
        TransactionModel.key(newName)
      ], { inconsistentRead })
    })
    expect(m1.id).toBe(this.modelName)
    expect(m2).toBe(undefined)

    const [m3, m4] = await db.Transaction.run(async (tx) => {
      return tx.get([
        TransactionModel.data(this.modelName),
        TransactionModel.data(newName)
      ], { inconsistentRead, createIfMissing: true })
    })
    expect(m3.id).toBe(this.modelName)
    expect(m4.id).toBe(newName)
  }

  testTrasactGet () {
    return this._testMultipleGet(false)
  }

  testBatchGet () {
    return this._testMultipleGet(true)
  }

  async testBatchGetUnprocessed () {
    const timeoutMock = jest.fn().mockImplementation((callback, after) => {
      callback()
    })
    const originalSetTimeout = setTimeout
    global.setTimeout = timeoutMock.bind(global)

    const batchGetMock = jest.fn().mockImplementation(() => {
      const ret = {
        Responses: {},
        UnprocessedKeys: {
          [TransactionModel.fullTableName]: { Keys: [{ _id: '456' }] }
        }
      }
      return {
        promise: async () => {
          return ret
        }
      }
    })
    const originalFunc = db.Transaction.prototype.documentClient.batchGet
    batchGetMock.bind(db.Transaction.prototype.documentClient)
    db.Transaction.prototype.documentClient.batchGet = batchGetMock

    const result = await db.Transaction.run(async tx => {
      const fut = tx.get([
        TransactionModel.key('123'),
        TransactionModel.key('456')
      ], { inconsistentRead: true })
      await expect(fut).rejects.toThrow('Failed to get all items')
      expect(batchGetMock).toHaveBeenCalledTimes(11)
      return 112233
    })
    expect(result).toBe(112233)

    db.Transaction.prototype.documentClient.batchGet = originalFunc
    global.setTimeout = originalSetTimeout
  }

  async testMultipleGet () {
    await db.Transaction.run(async (tx) => {
      const [m1, m2] = await tx.get([
        TransactionModel.data('a'),
        TransactionModel.data('b')
      ], { createIfMissing: true })
      const m3 = await tx.get(TransactionModel, 'c', { createIfMissing: true })
      const m4 = await tx.get(TransactionModel.data('d'),
        { createIfMissing: true })
      expect(m1.id).toBe('a')
      expect(m2.id).toBe('b')
      expect(m3.id).toBe('c')
      expect(m4.id).toBe('d')
    })
  }

  async testGetWithParams () {
    const params = { createIfMissing: true }
    await db.Transaction.run(async (tx) => {
      const [m1, m2] = await tx.get([
        TransactionModel.data('a'),
        TransactionModel.data('b')
      ], params)
      const m3 = await tx.get(TransactionModel, 'c', params)
      const m4 = await tx.get(TransactionModel.data('d'), params)
      const m5 = await tx.get(TransactionModel.key('e'))
      expect(m1.id).toBe('a')
      expect(m2.id).toBe('b')
      expect(m3.id).toBe('c')
      expect(m4.id).toBe('d')
      expect(m5).toBe(undefined)
    })
    await db.Transaction.run(async tx => {
      const m4NoCreateIfMissing = await tx.get(TransactionModel.key('d'))
      expect(m4NoCreateIfMissing.id).toBe('d')
      const m5 = await tx.get(TransactionModel.key('e'))
      expect(m5).toBe(undefined)
    })
  }

  async testEventualConsistentGet () {
    const msg = uuidv4()
    const params = { inconsistentRead: false, createIfMissing: true }
    const originalFunc = db.Model.__getParams
    const mock = jest.fn().mockImplementation((ignore, params) => {
      expect(params.inconsistentRead).toBe(false)
      // Hard to mock this properly,
      // so just throw with unique msg
      // and make sure it's caught outside
      throw new Error(msg)
    })
    db.Model.__getParams = mock

    const result = await db.Transaction.run(async (tx) => {
      const fut = tx.get(TransactionModel, 'c', params)
      await expect(fut).rejects.toThrow(msg)
      return 123
    })
    expect(result).toBe(123) // Prove the tx is ran

    db.Model.__getParams = originalFunc
  }
}

class TransactionWriteTest extends QuickTransactionTest {
  async beforeAll () {
    await super.beforeAll()
    this.modelName = '1234'
    await txGet(this.modelName, model => {
      model.field1 = 0
      model.field2 = 0
    })
  }

  async testWriteExisting () {
    const val = Math.floor(Math.random() * 999999)
    const data = TransactionModel.data(this.modelName)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(data, { createIfMissing: true })
      txModel.field1 = val
    })
    const model = await txGet(data)
    expect(model.field1).toBe(val)
  }

  async testWriteNew () {
    const modelName = uuidv4()
    const data = TransactionModel.data(modelName)
    const val = Math.floor(Math.random() * 999999)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(data, { createIfMissing: true })
      expect(txModel.isNew).toBe(true)
      txModel.field1 = val
    })
    const model = await txGet(data)
    expect(model.isNew).toBe(false)
    expect(model.field1).toBe(val)
  }

  async testMultipleCreateErrors () {
    const id1 = uuidv4()
    const id2 = uuidv4()
    function createBoth (tx) {
      tx.create(TransactionModel, { id: id1 })
      tx.create(TransactionModel, { id: id2 })
    }
    await db.Transaction.run(createBoth)
    expect((await txGet(id1)).id).toBe(id1)
    expect((await txGet(id2)).id).toBe(id2)
    try {
      await db.Transaction.run(createBoth)
      assert.fail('should not get here')
    } catch (err) {
      expect(err.message).toMatch(/^Multiple Unretryable Errors:/)
      expect(err.message).toContain('Tried to recreate an existing model: _id=' + id1)
      expect(err.message).toContain('Tried to recreate an existing model: _id=' + id2)
      expect(err.message.split('\n').length).toBe(3)
    }
  }

  async testCreateWithData () {
    const name = uuidv4()
    await db.Transaction.run(tx => {
      const model = tx.create(TransactionModel, { id: name, field1: 987 })
      model.field2 = 1
    })
    const model = await txGet(name)
    expect(model.field1).toBe(987)
  }

  async testWriteExistingAsNew () {
    const val = Math.floor(Math.random() * 999999)
    let tryCnt = 0
    const fut = db.Transaction.run({ retries: 3 }, async (tx) => {
      tryCnt++
      const txModel = tx.create(TransactionModel, { id: this.modelName })
      txModel.field1 = val
    })
    await expect(fut).rejects.toThrow(db.ModelAlreadyExistsError)
    expect(tryCnt).toBe(1)
  }

  async testReadContention () {
    // When updating, if properties read in a transaction was updated outside,
    // contention!
    const data = TransactionModel.data(uuidv4())
    const fut = db.Transaction.run({ retries: 0 }, async (tx) => {
      const txModel = await tx.get(data, { createIfMissing: true })
      await txGet(data, model => {
        model.field2 = 321
      })

      // Just reading a property that got changes outside of transaction
      // results in contention
      txModel.field2 // eslint-disable-line no-unused-expressions
      txModel.field1 = 123
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    const result = await txGet(data)
    expect(result.field2).toBe(321)
  }

  async testWriteContention () {
    // When updating, if properties change in a transaction was also updated
    // outside, contention!
    let result
    const data = TransactionModel.data(this.modelName)
    const fut = db.Transaction.run({ retries: 0 }, async (tx) => {
      const txModel = await tx.get(data, { createIfMissing: true })
      if (txModel.field1 && txModel.field2) {
        // no-op: just accessing the fields so we're conditioned on their
        // original values
      }

      await txGet(data, model => {
        model.field2 += 1
        result = model.field2
      })

      txModel.field2 = 111
      txModel.field1 = 123
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    const m = await txGet(data)
    expect(m.field2).toBe(result)
  }

  async testNoChangeNoWrite () {
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(TransactionModel, this.modelName,
        { createIfMissing: true })
      expect(txModel.isNew).toBe(false)

      expect(() => tx.__writeBatcher.__write(txModel)).toThrow()
      expect(tx.__writeBatcher.__toWrite.length).toBe(0)
    })
  }

  async testNewModelNoChange () {
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(TransactionModel, uuidv4(),
        { createIfMissing: true })
      expect(txModel.isNew).toBe(true)
      tx.__writeBatcher.__write(txModel)
      expect(tx.__writeBatcher.__toWrite.length).toBe(1)
      expect(tx.__writeBatcher.__toWrite[0]).toHaveProperty('Put')
    })
  }

  async testWriteSnapshot () {
    // Additional changes to model after call to update should not be reflected
    const data = TransactionModel.data(uuidv4())
    const deepObj = { a: 12 }
    await db.Transaction.run(async tx => {
      const model = await tx.get(data, { createIfMissing: true })
      expect(model.isNew).toBe(true)

      model.arrField = [deepObj]
      model.objField = { a: deepObj }
    })
    deepObj.a = 32
    const updated = await txGet(data)
    expect(updated.objField.a.a).toBe(12)
    expect(updated.arrField[0].a).toBe(12)
  }

  async testNoContention () {
    // When using update to write data, a weaker condition is used to check for
    // contention: If properties relevant to the transaction are modified,
    // there shouldn't be contention
    let finalVal
    const data = TransactionModel.data(this.modelName)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(data, { createIfMissing: true })
      const model = await txGet(data, model => {
        model.field2 += 1
      })

      txModel.field1 += 1
      finalVal = [txModel.field1, model.field2]
    })
    const updated = await txGet(data)
    expect(updated.field1).toBe(finalVal[0])
    expect(updated.field2).toBe(finalVal[1])
  }

  async testMismatchedKeysForCreateOrPut () {
    const id = uuidv4()
    const fut = db.Transaction.run(async tx => {
      tx.createOrPut(TransactionModel,
        { id },
        { id: id + 'x', field1: 3, field2: 1 })
    })
    await expect(fut).rejects.toThrow(db.InvalidParameterError)

    // can specify id in new data param (but it must match)
    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionModel,
        { id },
        { id, field1: 3, field2: 1, objField: { a: { a: 1 } } })
    })
    await db.Transaction.run(async tx => {
      const item = await tx.get(TransactionModel, id)
      expect(item.id).toBe(id)
      expect(item.field1).toBe(3)
      expect(item.field2).toBe(1)
      expect(item.objField).toEqual({ a: { a: 1 } })
    })

    // can omit id in new data param (it's implied)
    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionModel,
        { id, field1: 3 },
        { field1: 33, field2: 11, objField: { a: { a: 11 } } })
    })
    await db.Transaction.run(async tx => {
      const item = await tx.get(TransactionModel, id)
      expect(item.id).toBe(id)
      expect(item.field1).toBe(33)
      expect(item.field2).toBe(11)
      expect(item.objField).toEqual({ a: { a: 11 } })
    })
  }

  async testUpdateItemNonExisting () {
    const id = 'nonexist' + uuidv4()
    let fut = db.Transaction.run(async tx => {
      tx.update(TransactionModel,
        { id }, { field1: 2 })
    })
    await expect(fut).rejects.toThrow(Error)

    fut = db.Transaction.run(async tx => {
      tx.createOrPut(TransactionModelWithRequiredField,
        { id },
        { field1: 3, field2: 1 })
    })
    await expect(fut).rejects.toThrow(/missing required value/)

    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionModel,
        { id },
        { field1: 3, field2: 1, arrField: undefined, objField: undefined })
    })
    let model = await txGet(id)
    expect(model.field1).toBe(3)

    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionModel,
        { id },
        {
          field1: 3,
          field2: 567,
          arrField: undefined,
          objField: undefined
        })
    })
    model = await txGet(id)
    expect(model.field2).toBe(567)
  }

  async testUpdateNoReturn () {
    // UpdateItem should not return the model for futher modifications
    const fut = db.Transaction.run(async tx => {
      const ret = tx.update(TransactionModel,
        { id: this.modelName, field1: 1 }, { field1: 2 })
      expect(ret).toBe(undefined)
    })
    await expect(fut).rejects.toThrow()
  }

  async testUpdateConflict () {
    // Update fails when original data doesn't match db
    const fut = db.Transaction.run(async tx => {
      tx.update(TransactionModel,
        { id: this.modelName, field1: Math.floor(Math.random() * 9999999) },
        { field1: 2 }
      )
    })
    await expect(fut).rejects.toThrow()
  }

  async testUpdateInitialUndefined () {
    const fut = db.Transaction.run(async tx => {
      tx.update(
        TransactionModel,
        { id: uuidv4(), field1: undefined },
        { field1: 123 }
      )
    })
    await expect(fut).rejects.toThrow(db.InvalidParameterError)
  }

  async testUpdateItem () {
    const data = TransactionModel.data(this.modelName)
    const origModel = await txGet(data)
    const newVal = Math.floor(Math.random() * 9999999)
    await db.Transaction.run(async tx => {
      const original = {}
      Object.keys(TransactionModel.__VIS_ATTRS).forEach(fieldName => {
        const val = origModel[fieldName]
        if (val !== undefined) {
          original[fieldName] = val
        }
      })
      tx.update(data.Cls, original, { field1: newVal })
    })
    const updated = await txGet(data)
    expect(updated.field1).toBe(newVal)
  }

  async testUpdateWithID () {
    const fut = db.Transaction.run(async tx => {
      tx.update(
        TransactionModel,
        { id: this.modelName },
        { id: this.modelName })
    })
    await expect(fut).rejects.toThrow()
  }

  async testUpdateOtherFields () {
    await txGet(this.modelName, (m) => { m.field2 = 2 })
    await db.Transaction.run(async tx => {
      tx.update(
        TransactionModel,
        { id: this.modelName, field2: 2 },
        { field1: 1 })
    })
    const model = await txGet(this.modelName)
    expect(model.field1).toBe(1)
  }

  async testCreatePartialModel () {
    let fut = db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModelWithRequiredField,
        { id: this.modelName },
        {
          field1: 1,
          field2: 2,
          arrField: undefined,
          objField: undefined
        }
      )
    })
    await expect(fut).rejects.toThrow(/missing required value/)

    fut = db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModelWithRequiredField,
        { id: this.modelName },
        {
          field1: 1,
          field2: 2,
          arrField: undefined,
          objField: undefined,
          required: undefined
        }
      )
    })
    await expect(fut).rejects.toThrow(/missing required value/)

    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModelWithRequiredField,
        { id: this.modelName },
        {
          field1: 111222,
          field2: undefined,
          arrField: undefined,
          objField: undefined,
          required: 333444
        }
      )
    })
    const model = await txGetRequired(this.modelName)
    expect(model.field1).toBe(111222)
    expect(model.required).toBe(333444)
  }

  async testCreateNewModel () {
    // New model should work without conditions
    let name = uuidv4()
    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModel,
        { id: name },
        {
          field1: 333222,
          field2: undefined,
          arrField: undefined,
          objField: undefined
        }
      )
    })
    let model = await txGet(name)
    expect(model.field1).toBe(333222)

    // New model should work with conditions too
    name = uuidv4()
    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModel,
        { id: name, field1: 123123 },
        {
          field2: undefined,
          arrField: undefined,
          objField: undefined
        }
      )
    })
    model = await txGet(name)
    expect(model.field1).toBe(123123)
  }

  async testConditionalPut () {
    const name = uuidv4()
    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModel,
        { id: name },
        {
          field1: 9988234,
          field2: undefined,
          arrField: undefined,
          objField: undefined
        }
      )
    })
    let model = await txGet(name)
    expect(model.field1).toBe(9988234)

    const fut = db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModel,
        { id: name, field1: 123123 },
        {
          field2: 111,
          arrField: undefined,
          objField: undefined
        }
      )
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)

    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionModel,
        { id: name, field1: 9988234 },
        {
          field2: 111,
          arrField: undefined,
          objField: undefined
        }
      )
    })
    model = await txGet(name)
    expect(model.field1).toBe(9988234)
    expect(model.field2).toBe(111)
  }

  async testUpdatePartialModel () {
    // Make sure only fields to be updated are validated.
    const modelName = uuidv4()
    const fut = txGetRequired({ id: modelName })
    await expect(fut).rejects.toThrow() // Missing required field, should fail

    const data = { id: modelName, required: 1, field1: 1 }
    const model = await txGetRequired(data)
    const newVal = Math.floor(Math.random() * 99999999)
    await db.Transaction.run(async tx => {
      tx.update(
        TransactionModelWithRequiredField,
        { id: modelName, field1: model.field1 },
        { field1: newVal })
    })
    const updated = await txGetRequired({ id: modelName })
    expect(updated.field1).toBe(newVal)
  }

  async testEmptyUpdate () {
    const fut = db.Transaction.run(async tx => {
      tx.update(
        TransactionModel,
        { id: '123', field1: 1 },
        { })
    })
    await expect(fut).rejects.toThrow()
  }
}

class TransactionReadOnlyTest extends QuickTransactionTest {
  async testReadOnlyOption () {
    await expect(db.Transaction.run({ readOnly: true }, async tx => {
      tx.create(TransactionModel, { id: uuidv4() })
    })).rejects.toThrow('read-only')
  }

  async testMakeReadOnlyDuringTx () {
    await expect(db.Transaction.run(async tx => {
      tx.makeReadOnly()
      tx.update(TransactionModel, { id: uuidv4() }, { field1: 1 })
    })).rejects.toThrow('read-only')
  }
}

class TransactionRetryTest extends QuickTransactionTest {
  async expectRetries (err, maxTries, expectedRuns) {
    let cnt = 0
    const fut = db.Transaction.run({ retries: maxTries }, () => {
      cnt++
      throw err
    })
    await expect(fut).rejects.toThrow(Error)
    expect(cnt).toBe(expectedRuns)
  }

  async testRetryableErrors () {
    let err = new Error('something')
    await this.expectRetries(err, 0, 1)
    await this.expectRetries(err, 2, 1)

    err.retryable = true
    await this.expectRetries(err, 2, 3)

    err = new Error('fake')
    err.code = 'ConditionalCheckFailedException'
    await this.expectRetries(err, 1, 2)

    err.code = 'TransactionCanceledException'
    await this.expectRetries(err, 1, 2)
  }

  testIsRetrableErrors () {
    const err = new Error()
    expect(db.Transaction.__isRetryable(err)).toBe(false)

    err.name = 'TransactionCanceledException'
    expect(db.Transaction.__isRetryable(err)).toBe(false)

    err.code = 'TransactionCanceledException'
    expect(db.Transaction.__isRetryable(err)).toBe(true)
  }
}

class TransactionBackoffTest extends QuickTransactionTest {
  async checkBackoff (retries, expectedBackoff,
    initialBackoff, maxBackoff, expectedErr
  ) {
    initialBackoff = initialBackoff || 1
    maxBackoff = maxBackoff || 200
    expectedErr = expectedErr || db.TransactionFailedError
    const mock = jest.fn().mockImplementation((callback, after) => {
      callback()
    })
    const originalSetTimeout = setTimeout
    global.setTimeout = mock.bind(global)

    const originalRandom = Math.random
    Math.random = () => 0

    const err = new Error('')
    err.retryable = true
    const fut = db.Transaction.run({
      retries,
      initialBackoff,
      maxBackoff
    }, async tx => {
      throw err
    })
    await expect(fut).rejects.toThrow(expectedErr)
    expect(mock).toHaveBeenCalledTimes(retries)
    if (retries) {
      expect(mock).toHaveBeenLastCalledWith(expect.any(Function),
        expectedBackoff)
    }

    Math.random = originalRandom
    global.setTimeout = originalSetTimeout
  }

  async testExponentialBackoffs () {
    await this.checkBackoff(0)
    await this.checkBackoff(3, 3.6)
  }

  async testSmallMaxBackoff () {
    await this.checkBackoff(1, 90, 100, 200)
    await this.checkBackoff(0, undefined, 100, 199, db.InvalidOptionsError)
  }

  async testMaxBackoffs () {
    await this.checkBackoff(1, 90, 100, 200)
    await this.checkBackoff(3, 180, 100, 200)
  }
}

class TransactionConditionCheckTest extends QuickTransactionTest {
  async testReadModelTracking () {
    // Models read from transactions should be tracked
    await db.Transaction.run(async tx => {
      const models = []
      const model1 = await tx.get(TransactionModel, uuidv4(),
        { createIfMissing: true })
      models.push(model1)
      models.push(await tx.get(TransactionModel.data(uuidv4()),
        { createIfMissing: true }))
      const [model2, model3] = await tx.get([
        TransactionModel.data(uuidv4()),
        TransactionModel.data(uuidv4())
      ],
      { createIfMissing: true })
      models.push(model2)
      models.push(model3)

      const modelNames = new Set(models.map(m => m.toString()))
      let toCheckKeys = tx.__writeBatcher.__toCheck
      toCheckKeys = Object.keys(toCheckKeys)
        .filter(key => toCheckKeys[key] !== false)
      let result = new Set(toCheckKeys)
      expect(result).toStrictEqual(modelNames)

      function checkModel (m) {
        modelNames.delete(m.toString())
        toCheckKeys = tx.__writeBatcher.__toCheck
        toCheckKeys = Object.keys(toCheckKeys)
          .filter(key => toCheckKeys[key] !== false)
        result = new Set(toCheckKeys)
        expect(result).toStrictEqual(modelNames)
      }

      tx.__writeBatcher.__write(model1)
      checkModel(model1)

      model2.field1 = 0
      tx.__writeBatcher.__write(model2)
      checkModel(model2)
    })
  }
}

runTests(
  ParameterTest,
  TransactionEdgeCaseTest,
  TransactionGetTest,
  TransactionWriteTest,
  TransactionReadOnlyTest,
  TransactionRetryTest,
  TransactionBackoffTest,
  TransactionConditionCheckTest
)
