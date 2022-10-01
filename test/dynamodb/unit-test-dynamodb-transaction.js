const assert = require('assert')

const uuidv4 = require('uuid').v4

const db = require('../../src/dynamodb/src/default-db')
const S = require('../../src/schema/src/schema')
const { BaseTest, runTests } = require('../base-unit-test')

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
  return txGetGeneric(TransactionExample, keyValues, func)
}
async function txGetRequired (keyValues, func) {
  return txGetGeneric(TransactionExampleWithRequiredField, keyValues, func)
}

class TransactionExample extends db.Model {
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

  static INDEXES = {
    index1: { KEY: ['id'], SORT_KEY: ['field1', 'field2'], SPARSE: true },
    index2: { KEY: ['arrField'], SPARSE: true },
    index3: { KEY: ['id', 'field1'], SPARSE: true }
  }
}

class TransactionExampleWithRequiredField extends TransactionExample {
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
    await TransactionExample.createResources()
    await TransactionExampleWithRequiredField.createResources()
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

class KeyOnlyExample extends db.Model {
  static KEY = { id: S.str.min(1) }
  static SORT_KEY = { sk: S.str.min(1) }
}

class KeyOnlyExample2 extends KeyOnlyExample {
  static tableName = KeyOnlyExample.tableName // same
}

class TransactionEdgeCaseTest extends BaseTest {
  async beforeAll () {
    await KeyOnlyExample.createResources()
    await KeyOnlyExample2.createResources()
  }

  async afterEach () {
    jest.restoreAllMocks()
  }

  async testKeyCollision () {
    const suffix = uuidv4()
    await db.Transaction.run(tx => {
      const i1 = tx.create(KeyOnlyExample, { id: 'x', sk: 'y' + suffix })
      const i2 = tx.create(KeyOnlyExample, { id: 'xy', sk: suffix })
      expect(i1.toString()).not.toEqual(i2.toString())
    })
  }

  async testKeyCollisionFromSeparateModels () {
    const suffix = uuidv4()
    let checked = false
    const promise = db.Transaction.run(async tx => {
      const i1 = tx.create(KeyOnlyExample, { id: 'x', sk: suffix })
      await db.Transaction.run(tx => {
        const i2 = tx.create(KeyOnlyExample2, { id: 'x', sk: suffix })
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
      const item = await tx.get(KeyOnlyExample, idToRead)
      if (!item) {
        tx.create(KeyOnlyExample, idToWrite)
      }
    })
    await db.Transaction.run(async tx => {
      expect(await tx.get(KeyOnlyExample, idToRead)).toBe(undefined)
      expect(await tx.get(KeyOnlyExample, idToWrite)).not.toBe(undefined)
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
        TableName: 'sharedlibKeyOnlyExample',
        Item: {
          _id: idToWrite.id,
          _sk: 'y'
        },
        ConditionExpression: 'attribute_not_exists(#_id)',
        ExpressionAttributeNames: { '#_id': '_id' }
      }
    })
    expect(checkExpr).toEqual({
      ConditionCheck: {
        TableName: 'sharedlibKeyOnlyExample',
        Key: {
          _id: idToRead.id,
          _sk: 'x'
        },
        ConditionExpression: 'attribute_not_exists(#_id)',
        ExpressionAttributeNames: { '#_id': '_id' }
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
      await tx.get(TransactionExample, 'a',
        { createIfMissing: true })
      const fut = tx.get(TransactionExample, 'a',
        { createIfMissing: true })
      await expect(fut).rejects
        .toThrow('Model tracked for Get already tracked from Get: sharedlibTransactionExample _id=a')
    })
  }

  async testGetModelByID () {
    await db.Transaction.run(async (tx) => {
      const model = await tx.get(TransactionExample, 'a',
        { createIfMissing: true })
      expect(model.id).toBe('a')
    })
  }

  async testGetModelByKey () {
    await db.Transaction.run(async (tx) => {
      const model = await tx.get(TransactionExample.data('a'),
        { createIfMissing: true })
      expect(model.id).toBe('a')
    })
  }

  async testGetModelByKeys () {
    await db.Transaction.run(async (tx) => {
      const [m1, m2] = await tx.get([
        TransactionExample.data('a'),
        TransactionExample.data('b')
      ], { createIfMissing: true })
      expect(m1.id).toBe('a')
      expect(m2.id).toBe('b')
    })
  }

  async _testMultipleGet (inconsistentRead) {
    const newName = uuidv4()
    const [m1, m2] = await db.Transaction.run(async (tx) => {
      const ret = await tx.get([
        TransactionExample.key(this.modelName),
        TransactionExample.key(newName)
      ], { inconsistentRead })
      expect(tx.__writeBatcher.trackedModels.length).toBe(2)
      return ret
    })
    expect(m1.id).toBe(this.modelName)
    expect(m2).toBe(undefined)

    const [m3, m4] = await db.Transaction.run(async (tx) => {
      return tx.get([
        TransactionExample.data(this.modelName),
        TransactionExample.data(newName)
      ], { inconsistentRead, createIfMissing: true })
    })
    expect(m3.id).toBe(this.modelName)
    expect(m4.id).toBe(newName)
  }

  testTransactGet () {
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
          [TransactionExample.fullTableName]: { Keys: [{ _id: '456' }] }
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
        TransactionExample.key('123'),
        TransactionExample.key('456')
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
        TransactionExample.data('a'),
        TransactionExample.data('b')
      ], { createIfMissing: true })
      const m3 = await tx.get(TransactionExample, 'c', { createIfMissing: true })
      const m4 = await tx.get(TransactionExample.data('d'),
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
        TransactionExample.data('a'),
        TransactionExample.data('b')
      ], params)
      const m3 = await tx.get(TransactionExample, 'c', params)
      const m4 = await tx.get(TransactionExample.data('d'), params)
      const m5 = await tx.get(TransactionExample.key('e'))
      expect(m1.id).toBe('a')
      expect(m2.id).toBe('b')
      expect(m3.id).toBe('c')
      expect(m4.id).toBe('d')
      expect(m5).toBe(undefined)
    })
    await db.Transaction.run(async tx => {
      const m4NoCreateIfMissing = await tx.get(TransactionExample.key('d'))
      expect(m4NoCreateIfMissing.id).toBe('d')
      const m5 = await tx.get(TransactionExample.key('e'))
      expect(m5).toBe(undefined)
    })
  }

  async testInconsistentReadMissingEntry () {
    const ids = [uuidv4(), uuidv4(), uuidv4()]
    await db.Transaction.run(async tx => {
      await tx.create(TransactionExample, { id: ids[0] })
      await tx.create(TransactionExample, { id: ids[2] })
    })

    await db.Transaction.run(async tx => {
      const result = await tx.get([
        TransactionExample.key(ids[0]),
        TransactionExample.key(ids[1]),
        TransactionExample.key(ids[2])
      ],
      { inconsistentRead: true })

      expect(result[0].id).toEqual(ids[0])
      expect(result[1]).toBeUndefined()
      expect(result[2].id).toEqual(ids[2])
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
      const fut = tx.get(TransactionExample, 'c', params)
      await expect(fut).rejects.toThrow(msg)
      return 123
    })
    expect(result).toBe(123) // Prove the tx is ran

    db.Model.__getParams = originalFunc
  }

  async testGetMissingThenCreate () {
    let id = uuidv4()
    const ret = await db.Transaction.run(async tx => {
      const m1 = await tx.get(TransactionExample, id)
      const m2 = await tx.get(TransactionExample, id, { createIfMissing: true })
      return [m1, m2]
    })
    expect(ret[0]).toBe(undefined)
    expect(ret[1]._id).toBe(id)

    id = uuidv4()
    const fut = db.Transaction.run(async tx => {
      await tx.get(TransactionExample, id)
      tx.create(TransactionExample, { id })
    })
    await expect(fut).resolves.not.toThrow()
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
    const data = TransactionExample.data(this.modelName)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(data, { createIfMissing: true })
      txModel.field1 = val
      txModel.field2 = 200
    })
    const model = await txGet(data)
    expect(model.field1).toBe(val)
    expect(model._c_field1_field2).toBe([val, 200].join('\0'))
  }

  async testWriteNew () {
    const modelName = uuidv4()
    const data = TransactionExample.data(modelName)
    const val = Math.floor(Math.random() * 999999)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(data, { createIfMissing: true })
      expect(txModel.isNew).toBe(true)
      txModel.field1 = val
    })
    const model = await txGet(data)
    expect(model.isNew).toBe(false)
    expect(model.field1).toBe(val)
    expect(model._c_field1_field2).toBe(undefined)
    expect(model._c_field1_id).toBe([val, modelName].join('\0'))
  }

  async testWriteIndexData () {
    const data = TransactionExample.data(this.modelName)
    expect(async () => {
      await db.Transaction.run(async (tx) => {
        const txModel = await tx.get(data, { createIfMissing: true })
        txModel._c_field1_field2 = 'wrong data'
      })
    }).rejects.toThrow(db.InvalidFieldError)
  }

  async testMultipleCreateErrors () {
    const id1 = uuidv4()
    const id2 = uuidv4()
    function createBoth (tx) {
      tx.create(TransactionExample, { id: id1 })
      tx.create(TransactionExample, { id: id2 })
    }
    await db.Transaction.run(createBoth)
    expect((await txGet(id1)).id).toBe(id1)
    expect((await txGet(id2)).id).toBe(id2)
    try {
      await db.Transaction.run(createBoth)
      assert.fail('should not get here')
    } catch (err) {
      expect(err.message).toMatch(/^Multiple Non-retryable Errors:/)
      const errPrefix =
        'Tried to recreate an existing model: sharedlibTransactionExample _id='
      expect(err.message).toContain(errPrefix + id1)
      expect(err.message).toContain(errPrefix + id2)
      expect(err.message.split('\n').length).toBe(3)
    }
  }

  async testCreateWithData () {
    const name = uuidv4()
    await db.Transaction.run(tx => {
      const model = tx.create(TransactionExample, { id: name, field1: 987 })
      model.field2 = 1
    })
    const model = await txGet(name)
    expect(model.field1).toBe(987)
    expect(model._c_field1_field2).toBe([987, 1].join('\0'))
    expect(model._c_field1_id).toBe([987, name].join('\0'))
  }

  async testWriteExistingAsNew () {
    const val = Math.floor(Math.random() * 999999)
    let tryCnt = 0
    const fut = db.Transaction.run({ retries: 3 }, async (tx) => {
      tryCnt++
      const txModel = tx.create(TransactionExample, { id: this.modelName })
      txModel.field1 = val
    })
    await expect(fut).rejects.toThrow(db.ModelAlreadyExistsError)
    expect(tryCnt).toBe(1)
  }

  async testReadContention () {
    // When updating, if properties read in a transaction was updated outside,
    // contention!
    const data = TransactionExample.data(uuidv4())
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
    const data = TransactionExample.data(this.modelName)
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
      const txModel = await tx.get(TransactionExample, this.modelName,
        { createIfMissing: true })
      expect(txModel.isNew).toBe(false)

      expect(() => tx.__writeBatcher.__write(txModel)).toThrow()
      expect(tx.__writeBatcher.__toWrite.length).toBe(0)
    })
  }

  async testNewModelNoChange () {
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(TransactionExample, uuidv4(),
        { createIfMissing: true })
      expect(txModel.isNew).toBe(true)
      tx.__writeBatcher.__write(txModel)
      expect(tx.__writeBatcher.__toWrite.length).toBe(1)
      expect(tx.__writeBatcher.__toWrite[0]).toHaveProperty('Put')
    })
  }

  async testWriteSnapshot () {
    // Additional changes to model after call to update should not be reflected
    const data = TransactionExample.data(uuidv4())
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
    const data = TransactionExample.data(this.modelName)
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
      tx.createOrPut(TransactionExample,
        { id: id + 'x', field1: 3, field2: 1 },
        { id })
    })
    await expect(fut).rejects.toThrow(db.InvalidParameterError)

    // can specify id in new data param (but it must match)
    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionExample,
        { id, field1: 3, field2: 1, objField: { a: { a: 1 } } },
        { id })
    })
    await db.Transaction.run(async tx => {
      const item = await tx.get(TransactionExample, id)
      expect(item.id).toBe(id)
      expect(item.field1).toBe(3)
      expect(item.field2).toBe(1)
      expect(item.objField).toEqual({ a: { a: 1 } })
    })

    // can omit id in new data param (it's implied)
    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionExample,
        { field1: 33, field2: 11, objField: { a: { a: 11 } } },
        { id, field1: 3 })
    })
    await db.Transaction.run(async tx => {
      const item = await tx.get(TransactionExample, id)
      expect(item.id).toBe(id)
      expect(item.field1).toBe(33)
      expect(item.field2).toBe(11)
      expect(item.objField).toEqual({ a: { a: 11 } })
    })
  }

  async testUpdateItemNonExisting () {
    const id = 'nonexistent' + uuidv4()
    let fut = db.Transaction.run(async tx => {
      tx.update(TransactionExample,
        { id }, { field1: 2 })
    })
    await expect(fut).rejects.toThrow(Error)

    fut = db.Transaction.run(async tx => {
      tx.createOrPut(TransactionExampleWithRequiredField,
        { field1: 3, field2: 1 },
        { id })
    })
    await expect(fut).rejects.toThrow(/missing required value/)

    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionExample,
        { field1: 3, field2: 1, arrField: undefined, objField: undefined },
        { id })
    })
    let model = await txGet(id)
    expect(model.field1).toBe(3)

    await db.Transaction.run(async tx => {
      tx.createOrPut(TransactionExample,
        {
          field1: 3,
          field2: 567,
          arrField: undefined,
          objField: undefined
        },
        { id }
      )
    })
    model = await txGet(id)
    expect(model.field2).toBe(567)
  }

  async testUpdateNoReturn () {
    // UpdateItem should not return the model for further modifications
    const fut = db.Transaction.run(async tx => {
      const ret = tx.update(TransactionExample,
        { id: this.modelName, field1: 1 }, { field1: 2 })
      expect(ret).toBe(undefined)
    })
    await expect(fut).rejects.toThrow()
  }

  async testUpdateConflict () {
    // Update fails when original data doesn't match db
    const fut = db.Transaction.run(async tx => {
      tx.update(TransactionExample,
        { id: this.modelName, field1: Math.floor(Math.random() * 9999999) },
        { field1: 2 }
      )
    })
    await expect(fut).rejects.toThrow()
  }

  async testUpdateInitialUndefined () {
    const fut = db.Transaction.run(async tx => {
      tx.update(
        TransactionExample,
        { id: uuidv4(), field1: undefined },
        { field1: 123 }
      )
    })
    await expect(fut).rejects.toThrow(db.InvalidParameterError)
  }

  async testUpdateItem () {
    const data = TransactionExample.data(this.modelName)
    const origModel = await txGet(data)
    const newVal = Math.floor(Math.random() * 9999999)
    await db.Transaction.run(async tx => {
      const original = {}
      Object.keys(TransactionExample._attrs).forEach(fieldName => {
        const val = origModel[fieldName]
        if (val !== undefined) {
          original[fieldName] = val
        }
      })
      tx.update(data.Cls, original, { field1: newVal })
    })
    const updated = await txGet(data)
    expect(updated.field1).toBe(newVal)
    expect(updated._c_field1_id).toBe([newVal, this.modelName].join('\0'))
  }

  async testUpdateWithID () {
    const fut = db.Transaction.run(async tx => {
      tx.update(
        TransactionExample,
        { id: this.modelName },
        { id: this.modelName })
    })
    await expect(fut).rejects.toThrow()
  }

  async testUpdateOtherFields () {
    await txGet(this.modelName, (m) => { m.field2 = 2 })
    await db.Transaction.run(async tx => {
      tx.update(
        TransactionExample,
        { id: this.modelName, field2: 2 },
        { field1: 1 })
    })
    const model = await txGet(this.modelName)
    expect(model.field1).toBe(1)
    expect(model._c_field1_field2).toBe([1, 2].join('\0'))
    expect(model._c_field1_id).toBe([1, this.modelName].join('\0'))
  }

  async testDeleteFieldByUpdate () {
    await txGet(this.modelName, (m) => { m.field2 = 2 })
    await db.Transaction.run(async tx => {
      tx.update(
        TransactionExample,
        { id: this.modelName, field2: 2 },
        { field2: undefined })
    })
    const model = await txGet(this.modelName)
    expect(model.field2).toBe(undefined)
    expect(model._field1_field2).toBe(undefined)
  }

  async testCreatePartialModel () {
    let fut = db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionExampleWithRequiredField,
        {
          field1: 1,
          field2: 2,
          arrField: undefined,
          objField: undefined
        },
        { id: this.modelName }
      )
    })
    await expect(fut).rejects.toThrow(/missing required value/)

    fut = db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionExampleWithRequiredField,
        {
          field1: 1,
          field2: 2,
          arrField: undefined,
          objField: undefined,
          required: undefined
        },
        { id: this.modelName }
      )
    })
    await expect(fut).rejects.toThrow(/missing required value/)

    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionExampleWithRequiredField,
        {
          id: this.modelName,
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
    expect(model._c_field1_id).toBe([111222, this.modelName].join('\0'))
  }

  async testCreateNewModel () {
    // New model should work without conditions
    let name = uuidv4()
    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionExample,
        {
          id: name,
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
        TransactionExample,
        {
          field2: undefined,
          arrField: undefined,
          objField: undefined
        },
        { id: name, field1: 123123 }
      )
    })
    model = await txGet(name)
    expect(model.field1).toBe(123123)
  }

  async testConditionalPut () {
    const name = uuidv4()
    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionExample,
        {
          field1: 9988234,
          field2: undefined,
          arrField: undefined,
          objField: undefined
        },
        { id: name }
      )
    })
    let model = await txGet(name)
    expect(model.field1).toBe(9988234)

    const fut = db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionExample,
        {
          field2: 111,
          arrField: undefined,
          objField: undefined
        },
        { id: name, field1: 123123 } // initial value doesn't match
      )
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)

    await db.Transaction.run(async tx => {
      tx.createOrPut(
        TransactionExample,
        {
          field2: 111,
          arrField: undefined,
          objField: undefined
        },
        { id: name, field1: 9988234 } // initial value ok
      )
    })
    model = await txGet(name)
    expect(model.field1).toBe(9988234)
    expect(model.field2).toBe(111)
  }

  async testTransactionalCreateOrPut () {
    const ids = [uuidv4(), uuidv4()]
    const helper = async (value) => {
      await db.Transaction.run(async tx => {
        for (const id of ids) {
          tx.createOrPut(
            TransactionExample,
            {
              id,
              field1: value,
              field2: 111,
              arrField: undefined,
              objField: undefined
            }
          )
        }
      })
      for (const id of ids) {
        const model = await txGet(id)
        expect(model).toBeDefined()
        expect(model.field1).toBe(value)
      }
    }
    await helper(1)
    await helper(2)
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
        TransactionExampleWithRequiredField,
        { id: modelName, field1: model.field1 },
        { field1: newVal })
    })
    const updated = await txGetRequired({ id: modelName })
    expect(updated.field1).toBe(newVal)
    expect(updated._c_field1_id).toBe([newVal, modelName].join('\0'))
  }

  async testEmptyUpdate () {
    const fut = db.Transaction.run(async tx => {
      tx.update(
        TransactionExample,
        { id: '123', field1: 1 },
        { })
    })
    await expect(fut).rejects.toThrow()
  }

  /**
   * Verify model cannot be tracked more than once inside a tx.
   */
  async testDuplicateTracking () {
    // verify create then get on non existing item fails
    let future = db.Transaction.run(async tx => {
      tx.createOrPut(TransactionExample, { id: 'abc', field1: 1 })
      await tx.get(TransactionExample, { id: 'abc' })
    })
    await expect(future)
      .rejects
      .toThrow(
        'Model tracked for Get already tracked from CreateOrPut: sharedlibTransactionExample _id=abc'
      )

    // verify delete then get fails
    future = db.Transaction.run(async tx => {
      tx.delete(TransactionExample.key({ id: 'abc' }))
      await tx.get(TransactionExample, { id: 'abc' })
    })
    await expect(future)
      .rejects
      .toThrow(
        'Model tracked for Get already tracked from Delete: sharedlibTransactionExample _id=abc'
      )
  }
}

class TransactionReadOnlyTest extends QuickTransactionTest {
  async testReadOnlyOption () {
    await expect(db.Transaction.run({ readOnly: true }, async tx => {
      tx.create(TransactionExample, { id: uuidv4() })
    })).rejects.toThrow('read-only')
  }

  async testMakeReadOnlyDuringTx () {
    await expect(db.Transaction.run(async tx => {
      tx.makeReadOnly()
      tx.update(TransactionExample, { id: uuidv4() }, { field1: 1 })
    })).rejects.toThrow('read-only')
  }

  async testDelete () {
    await expect(db.Transaction.run(async tx => {
      tx.makeReadOnly()
      tx.delete(TransactionExample.key({ id: uuidv4() }))
    })).rejects.toThrow('in a read-only transaction')
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

  testIsRetryableErrors () {
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
      const model1 = await tx.get(TransactionExample, uuidv4(),
        { createIfMissing: true })
      models.push(model1)
      models.push(await tx.get(TransactionExample.data(uuidv4()),
        { createIfMissing: true }))
      const [model2, model3] = await tx.get([
        TransactionExample.data(uuidv4()),
        TransactionExample.data(uuidv4())
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

  async testModelExistence () {
    // Even if a model was only read, but no properties are accessed, a
    // condition should be generated when the tx commits
    const __WriteBatcher = db.__private.__WriteBatcher
    const spy = jest.spyOn(__WriteBatcher.prototype, 'transactWrite')
    const id = uuidv4()

    // Non-existent model
    await db.Transaction.run(async tx => {
      await tx.get(TransactionExample, uuidv4())
      tx.create(TransactionExample, { id })
    })
    expect(spy).toHaveBeenCalledTimes(1)
    let callArgs = spy.mock.calls[0]
    expect(callArgs.length).toBe(1)
    expect(callArgs[0].TransactItems[1].ConditionCheck.ConditionExpression)
      .toBe('attribute_not_exists(#_id)')

    spy.mockReset()
    // Existing model
    await db.Transaction.run(async tx => {
      await tx.get(TransactionExample, id)
      tx.create(TransactionExample, { id: uuidv4() })
    })
    expect(spy).toHaveBeenCalledTimes(1)
    callArgs = spy.mock.calls[0]
    expect(callArgs.length).toBe(1)
    expect(callArgs[0].TransactItems[1].ConditionCheck.ConditionExpression)
      .toBe('attribute_exists(#_id)')
    spy.mockRestore()
  }
}

class TransactionDeleteTest extends QuickTransactionTest {
  async getNoCreate (id) {
    return db.Transaction.run(tx => {
      return tx.get(TransactionExample, id)
    })
  }

  async testDeleteParams () {
    const result = await db.Transaction.run(async tx => {
      const m1 = await tx.get(TransactionExample, uuidv4(),
        { createIfMissing: true })
      const m2 = await tx.get(TransactionExample, uuidv4(),
        { createIfMissing: true })
      const m3 = TransactionExample.key({ id: uuidv4() })

      tx.delete(m1, m2, m3) // fine

      expect(() => {
        tx.delete(123)
      }).toThrow('Invalid parameter args. Must be models and keys.')

      return 1122331
    })
    expect(result).toBe(1122331) // Proof that tx ran
  }

  async testDeleteModel () {
    const m = await txGet(uuidv4())
    const key = TransactionExample.key({ id: m.id })
    const result = await db.Transaction.run(async tx => {
      const model = await tx.get(key)
      tx.delete(model)
      return model
    })
    expect(result.id).toBe(m.id)
    expect(await this.getNoCreate(m.id)).toBeUndefined()
  }

  async testTxDeleteModel () {
    const m = await txGet(uuidv4())
    const key = TransactionExample.key({ id: m.id })
    const result = await db.Transaction.run(async tx => {
      // multiple items goes through TransactWrite
      await tx.get(TransactionExample, uuidv4(), { createIfMissing: true })
      const model = await tx.get(key)
      tx.delete(model)
      return model
    })
    expect(result.id).toBe(m.id)
    expect(await this.getNoCreate(m.id)).toBeUndefined()
  }

  async testDeleteNonExisting () {
    // Deleting an item that we don't know if exists should silently pass
    const data = TransactionExample.data({ id: uuidv4() })
    await db.Transaction.run(async tx => {
      tx.delete(data)
    })

    await db.Transaction.run(async tx => {
      // creat then delete in the same transaction don't cause conflicts
      const model = await tx.get(data, { createIfMissing: true })
      tx.delete(model)
    })

    await db.Transaction.run(async tx => {
      // creat then delete in the same transaction don't cause conflicts
      const model = tx.create(data.Cls, data.keyComponents)
      tx.delete(model)
    })
  }

  async testDeleteMissing () {
    // Deleting an item that we DO know exists should fail
    const key = TransactionExample.key({ id: uuidv4() })
    await txGet(key.keyComponents.id)
    let fut = db.Transaction.run({ retries: 0 }, async tx => {
      const model = await tx.get(key)
      await db.Transaction.run(async innerTx => {
        innerTx.delete(key)
      })
      tx.delete(model)
    })
    await expect(fut).rejects.toThrow(
      'Tried to delete model with outdated / invalid conditions:')

    await txGet(key.keyComponents.id)
    fut = db.Transaction.run({ retries: 0 }, async tx => {
      await tx.get(TransactionExample, uuidv4())
      const model = await tx.get(key)
      await db.Transaction.run(async innerTx => {
        innerTx.delete(key)
      })
      tx.delete(model)
    })
    await expect(fut).rejects.toThrow(
      'Tried to delete model with outdated / invalid conditions:')
  }

  async testMissingRequired () {
    // Deleting using key should work even when the model has required fields
    await db.Transaction.run({ retries: 0 }, async tx => {
      tx.delete(TransactionExampleWithRequiredField.key({ id: uuidv4() }))
    })
  }

  async testDoubleDeletion () {
    const id = uuidv4()
    let fut = db.Transaction.run({ retries: 0 }, async tx => {
      tx.delete(TransactionExample.key({ id }))
      tx.delete(TransactionExample.key({ id }))
    })
    await expect(fut).rejects.toThrow(
      'Tried to delete model when it\'s already deleted in the current tx:')

    fut = db.Transaction.run({ retries: 0 }, async tx => {
      const model = await tx.get(TransactionExample.data({ id }),
        { createIfMissing: true })
      tx.delete(model)
      tx.delete(model)
    })
    await expect(fut).rejects.toThrow(
      'Tried to delete model when it\'s already deleted in the current tx:')
  }

  /**
   * Verify delete fails if accessed fields are modified during tx
   */
  async testAccessedFieldModified () {
    const id = uuidv4()
    await txGet(id, m => {
      m.field1 = 123
    })

    const fut = db.Transaction.run({ retries: 0 }, async tx => {
      const model = await tx.get(TransactionExample, id)
      if (model.field1 === 123 && model.id === id) {
        tx.delete(model)
      }
      await db.Transaction.run(async innerTx => {
        // accessed in outer tx, should fail outer delete
        const model2 = await innerTx.get(TransactionExample, id)
        model2.field1 = 321
      })
    })
    await expect(fut).rejects.toThrow(
      'Tried to delete model with outdated / invalid conditions')
  }

  /**
   * Verify delete succeeds even if unaccessed fields are modified during tx
   */
  async testUnaccessedFieldModified () {
    const id = uuidv4()
    await txGet(id, m => {
      m.field1 = 123
    })

    const fut = db.Transaction.run({ retries: 0 }, async tx => {
      const model = await tx.get(TransactionExample, id)
      if (model.field1 === 123 && model.id === id) {
        tx.delete(model)
      }

      await db.Transaction.run(async innerTx => {
        const innerModel = await innerTx.get(TransactionExample, id)

        // Not accessed in outer tx, should not fail outer delete
        innerModel.field2 = 1000
      })
    })

    await expect(fut).resolves.not.toThrow()
  }

  /**
   * Verify delete succeeds even if written but not read
   * fields are modified during tx
   */
  async testReadlessAccessedFieldModified () {
    const id = uuidv4()
    await txGet(id, m => {
      m.field1 = 123
    })
    const delPromise = db.Transaction.run({ retries: 0 }, async tx => {
      const model = await tx.get(TransactionExample, id)
      model.getField('field1').incrementBy(1)

      await db.Transaction.run(async innerTx => {
        // current value of field should be ignored
        const innerModel = await innerTx.get(TransactionExample, id)
        innerModel.getField('field1').incrementBy(2)
      })
      tx.delete(model)
    })
    await expect(delPromise).resolves.not.toThrow()
  }
}

class TransactionCacheModelsTest extends BaseTest {
  async testGetOne () {
    const id = uuidv4()
    const ret = await db.Transaction.run({ cacheModels: true }, async tx => {
      const m1 = await tx.get(TransactionExample, id, { createIfMissing: true })
      const m2 = await tx.get(TransactionExample, id)
      return [m1, m2]
    })
    expect(ret[0]._id).toBe(ret[1]._id)
  }

  async testGetMissing () {
    const id = uuidv4()
    const ret = await db.Transaction.run({ cacheModels: true }, async tx => {
      const m1 = await tx.get(TransactionExample, id)
      // Repeatedly getting a missing item should also work
      const m2 = await tx.get(TransactionExample, id)
      const m3 = await tx.get(TransactionExample, id, { createIfMissing: true })
      return [m1, m2, m3]
    })
    expect(ret[0]).toBe(undefined)
    expect(ret[1]).toBe(undefined)
    expect(ret[2]._id).toBe(id)
  }

  async testGetMany () {
    const helper = async (inconsistentRead) => {
      const id = uuidv4()
      const ret = await db.Transaction.run(async tx => {
        tx.enableModelCache()
        const opts = { createIfMissing: true, inconsistentRead }
        const m2 = await tx.get(TransactionExample, id, opts)
        const ms = await tx.get([
          TransactionExample.data({ id: uuidv4() }),
          TransactionExample.data({ id })
        ], opts)
        return [ms[1], m2]
      })
      expect(ret[0]._id).toBe(ret[1]._id)
    }
    await helper(true) // batchGet
    await helper(false) // transactGet
  }

  async testDeletedModels () {
    const id = uuidv4()
    let fut = db.Transaction.run({ cacheModels: true }, async tx => {
      const opts = { createIfMissing: true }
      const model = await tx.get(TransactionExample.data({ id }), opts)
      tx.delete(model)
      await tx.get(TransactionExample.key({ id }))
    })
    await expect(fut).rejects.toThrow('Model is not a valid cached model')

    fut = db.Transaction.run({ cacheModels: true }, async tx => {
      const opts = { createIfMissing: true }
      tx.delete(TransactionExample.key({ id }))
      await tx.get(TransactionExample, id, opts)
    })
    await expect(fut).rejects.toThrow('Model is not a valid cached model')
  }

  async testPutModels () {
    // Models created with createOrPut cannot be read and modified afterwards
    const id = uuidv4()
    const fut = db.Transaction.run({ cacheModels: true }, async tx => {
      tx.createOrPut(TransactionExample,
        { id, field1: 3 }
      )
      await tx.get(TransactionExample.key({ id }))
    })
    await expect(fut).rejects.toThrow('Model is not a valid cached model')
  }

  async testCreateModels () {
    const id = uuidv4()
    const fut = db.Transaction.run({ cacheModels: true }, async tx => {
      tx.create(TransactionExample, { id })
      await tx.get(TransactionExample.key({ id }))
    })
    await expect(fut).rejects.toThrow('Model is not a valid cached model')
  }

  async testPersistedChanges () {
    const id = uuidv4()
    const res = await db.Transaction.run({ cacheModels: true }, async tx => {
      const model = await tx.get(TransactionExample.data({ id }),
        { createIfMissing: true })
      model.field1 = 1.1
      return tx.get(TransactionExample.key({ id }))
    })
    expect(res.field1).toBe(1.1)
  }
}

class ModelDiffsTest extends BaseTest {
  get defaultExpectation () {
    return {
      TransactionExample: {
        _id: undefined,
        data: {
          _id: undefined,
          field1: undefined,
          field2: undefined,
          arrField: undefined,
          objField: undefined,
          _c_field1_field2: undefined,
          _c_field1_id: undefined,
          _c_arrField: undefined
        }
      }
    }
  }

  async testNonexistent () {
    const id = uuidv4()
    const result = await db.Transaction.run(async tx => {
      await tx.get(TransactionExample, id)
      return tx.getModelDiffs()
    })
    expect(result.before).toStrictEqual([{
      TransactionExample: {
        _id: id,
        data: undefined
      }
    }])
    expect(result.after).toStrictEqual([{
      TransactionExample: {
        _id: id,
        data: undefined
      }
    }])
  }

  async testGet () {
    const id = uuidv4()
    const result = await db.Transaction.run(async tx => {
      const m = await tx.get(TransactionExample, id, { createIfMissing: true })
      m.field1 = 321
      return tx.getModelDiffs()
    })
    const expectation = this.defaultExpectation
    expectation.TransactionExample._id = id
    expect(result.before[0]).toStrictEqual(expectation)

    expectation.TransactionExample.data._id = id
    expectation.TransactionExample.data.field1 = 321
    expectation.TransactionExample.data._c_field1_id = [321, id].join('\0')
    expect(result.after[0]).toStrictEqual(expectation)
  }

  async __helperTestGet (func) {
    const ids = [uuidv4(), uuidv4()]
    const result = await db.Transaction.run(async tx => {
      await func(tx, ids)
      return tx.getModelDiffs()
    })

    // confirm _ids appropriately added for before/after snapshots.
    const expectedModels = [this.defaultExpectation, this.defaultExpectation]
    expectedModels[0].TransactionExample._id = ids[0]
    expectedModels[1].TransactionExample._id = ids[1]
    expect(result.before.length).toEqual(2)
    expect(result.before).toEqual(expect.arrayContaining(expectedModels))

    expectedModels[0].TransactionExample.data._id = ids[0]
    expectedModels[1].TransactionExample.data._id = ids[1]
    expect(result.after.length).toEqual(2)
    expect(result.after).toEqual(expect.arrayContaining(expectedModels))

    // verify that no additional properties were included/excluded.
    for (const entry of [...result.before, ...result.after]) {
      // already validated _id, so we can safely set.
      entry.TransactionExample._id = undefined
      entry.TransactionExample.data._id = undefined
      expect(entry).toStrictEqual(this.defaultExpectation)
    }
  }

  async testMultipleGets () {
    await this.__helperTestGet(async (tx, ids) => {
      return Promise.all(ids.map(id => {
        return tx.get(TransactionExample, id, { createIfMissing: true })
      }))
    })
  }

  async testDelete () {
    const id = uuidv4()
    // Blind delete
    const result = await db.Transaction.run(async tx => {
      await tx.delete(TransactionExample.key({ id }))
      return tx.getModelDiffs()
    })
    const expectation = this.defaultExpectation
    expectation.TransactionExample._id = id
    expect(result).toStrictEqual({
      before: [expectation],
      after: [{
        TransactionExample: {
          _id: id,
          data: undefined
        }
      }],
      diff: [{
        TransactionExample: {
          _id: id,
          data: {
            added: {},
            deleted: {},
            updated: undefined
          }
        }
      }]
    })

    // Create model
    await db.Transaction.run(async tx => {
      await tx.get(TransactionExample, { id, field1: 1 },
        { createIfMissing: true })
    })
    const result2 = await db.Transaction.run(async tx => {
      const m = await tx.get(TransactionExample, id)
      await tx.delete(m)
      return tx.getModelDiffs()
    })
    expectation.TransactionExample.data._id = id
    expectation.TransactionExample.data.field1 = 1
    expectation.TransactionExample.data._c_field1_id = [1, id].join('\0')
    expect(result2).toStrictEqual({
      before: [expectation],
      after: [{
        TransactionExample: {
          _id: id,
          data: undefined
        }
      }],
      diff: [{
        TransactionExample: {
          _id: id,
          data: {
            added: {},
            deleted: {},
            updated: undefined
          }
        }
      }]
    })
  }

  async testTransactGet () {
    await this.__helperTestGet(async (tx, ids) => {
      return tx.get(ids.map(id => TransactionExample.data({ id })),
        { createIfMissing: true })
    })
  }

  async testBatchGet () {
    await this.__helperTestGet(async (tx, ids) => {
      return tx.get(ids.map(id => TransactionExample.data({ id })),
        { createIfMissing: true, inconsistentRead: true })
    })
  }
}

runTests(
  ParameterTest,
  TransactionBackoffTest,
  TransactionConditionCheckTest,
  TransactionDeleteTest,
  TransactionEdgeCaseTest,
  TransactionGetTest,
  TransactionReadOnlyTest,
  TransactionRetryTest,
  TransactionWriteTest,
  TransactionCacheModelsTest,
  ModelDiffsTest
)
