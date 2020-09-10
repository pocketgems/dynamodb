const S = require('fluent-schema')
const uuidv4 = require('uuid').v4

const { BaseTest, runTests } = require('./base-unit-test')
const db = require('../src/dynamodb')

async function txGetGeneric (cls, keyValues, func) {
  return db.Transaction.run(async tx => {
    let model
    if (keyValues.constructor.name === 'Key') {
      model = await tx.get(keyValues, { createIfMissing: true })
    } else {
      model = await tx.get(cls, keyValues, { createIfMissing: true })
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
  static KEY = { id: S.string().minLength(1) }
  static FIELDS = {
    field1: S.number().optional(),
    field2: S.number().optional(),
    arrField: S.array().optional(),
    objField: S.object().optional()
  }
}
class TransactionModelWithRequiredField extends TransactionModel {
  static FIELDS = { ...super.FIELDS, required: S.number() }
}

class QuickTransactionTest extends BaseTest {
  mockTransactionDefaultOptions (options) {
    Object.defineProperty(db.Transaction.prototype, 'defaultOptions', {
      value: options,
      writable: false
    })
  }

  async setUp () {
    await super.setUp()
    await TransactionModel.createUnittestResource()
    await TransactionModelWithRequiredField.createUnittestResource()
    this.oldTransactionOptions = db.Transaction.prototype.defaultOptions
    const newOptions = Object.assign({}, this.oldTransactionOptions)
    Object.assign(newOptions, { retries: 1, initialBackoff: 20 })
    this.mockTransactionDefaultOptions(newOptions)
  }

  async tearDown () {
    super.tearDown()
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

class TransactionGetTest extends QuickTransactionTest {
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
      const model = await tx.get(TransactionModel.key('a'),
        { createIfMissing: true })
      expect(model.id).toBe('a')
    })
  }

  async testGetModelByKeys () {
    await db.Transaction.run(async (tx) => {
      const [m1, m2] = await tx.get([
        TransactionModel.key('a'),
        TransactionModel.key('b')
      ], { createIfMissing: true })
      expect(m1.id).toBe('a')
      expect(m2.id).toBe('b')
    })
  }

  async testMultipleGet () {
    await db.Transaction.run(async (tx) => {
      const [m1, m2] = await tx.get([
        TransactionModel.key('a'),
        TransactionModel.key('b')
      ], { createIfMissing: true })
      const m3 = await tx.get(TransactionModel, 'c', { createIfMissing: true })
      const m4 = await tx.get(TransactionModel.key('d'),
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
        TransactionModel.key('a'),
        TransactionModel.key('b')
      ], params)
      const m3 = await tx.get(TransactionModel, 'c', params)
      const m4 = await tx.get(TransactionModel.key('d'), params)
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
    const originalFunc = TransactionModel.prototype.__getParams
    const mock = jest.fn().mockImplementation((ignore, params) => {
      expect(params.inconsistentRead).toBe(false)
      // Hard to mock this properly,
      // so just throw with unique msg
      // and make sure it's caught outside
      throw new Error(msg)
    })
    TransactionModel.prototype.__getParams = mock

    const result = await db.Transaction.run(async (tx) => {
      const fut = tx.get(TransactionModel, 'c', params)
      await expect(fut).rejects.toThrow(msg)
      return 123
    })
    expect(result).toBe(123) // Prove the tx is ran

    TransactionModel.prototype.__getParams = originalFunc
  }
}

class TransactionWriteTest extends QuickTransactionTest {
  async setUp () {
    await super.setUp()
    this.modelName = '1234'
    await txGet(this.modelName, model => {
      model.field1 = 0
      model.field2 = 0
    })
  }

  async testWriteExisting () {
    const val = Math.floor(Math.random() * 999999)
    const key = TransactionModel.key(this.modelName)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(key, { createIfMissing: true })
      txModel.field1 = val
    })
    const model = await txGet(key)
    expect(model.field1).toBe(val)
  }

  async testWriteNew () {
    const modelName = uuidv4()
    const key = TransactionModel.key(modelName)
    const val = Math.floor(Math.random() * 999999)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(key, { createIfMissing: true })
      expect(txModel.isNew).toBe(true)
      txModel.field1 = val
    })
    const model = await txGet(key)
    expect(model.isNew).toBe(false)
    expect(model.field1).toBe(val)
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
    const key = TransactionModel.key(uuidv4())
    const fut = db.Transaction.run({ retries: 0 }, async (tx) => {
      const txModel = await tx.get(key, { createIfMissing: true })
      await txGet(key, model => {
        model.field2 = 321
      })

      // Just reading a property that got changes outside of transaction
      // results in contention
      txModel.field2 // eslint-disable-line no-unused-expressions
      txModel.field1 = 123
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    const result = await txGet(key)
    expect(result.field2).toBe(321)
  }

  async testWriteContention () {
    // When updating, if properties change in a transaction was also updated
    // outside, contention!
    let result
    const key = TransactionModel.key(this.modelName)
    const fut = db.Transaction.run({ retries: 0 }, async (tx) => {
      const txModel = await tx.get(key, { createIfMissing: true })
      await txGet(key, model => {
        model.field2 += 1
        result = model.field2
      })

      txModel.field2 = 111
      txModel.field1 = 123
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    const m = await txGet(key)
    expect(m.field2).toBe(result)
  }

  async testNoChangeNoWrite () {
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(TransactionModel, this.modelName,
        { createIfMissing: true })
      expect(txModel.isNew).toBe(false)

      await expect(tx.__writeBatcher.__write(txModel)).rejects.toThrow()
      expect(tx.__writeBatcher.__toWrite.length).toBe(0)
    })
  }

  async testNewModelNoChange () {
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(TransactionModel, uuidv4(),
        { createIfMissing: true })
      expect(txModel.isNew).toBe(true)
      await tx.__writeBatcher.__write(txModel)
      expect(tx.__writeBatcher.__toWrite.length).toBe(1)
      expect(tx.__writeBatcher.__toWrite[0]).toHaveProperty('Put')
    })
  }

  async testWriteSnapshot () {
    // Additional changes to model after call to update should not be reflected
    const key = TransactionModel.key(uuidv4())
    const deepObj = { a: 12 }
    await db.Transaction.run(async tx => {
      const model = await tx.get(key, { createIfMissing: true })
      expect(model.isNew).toBe(true)

      model.arrField = [deepObj]
      model.objField = { a: deepObj }
    })
    deepObj.a = 32
    const updated = await txGet(key)
    expect(updated.objField.a.a).toBe(12)
    expect(updated.arrField[0].a).toBe(12)
  }

  async testNoContention () {
    // When using update to write data, a weaker condition is used to check for
    // contention: If properties relavant to the transaction are modified,
    // there shouldn't be contention
    let finalVal
    const key = TransactionModel.key(this.modelName)
    await db.Transaction.run(async (tx) => {
      const txModel = await tx.get(key, { createIfMissing: true })
      const model = await txGet(key, model => {
        model.field2 += 1
      })

      txModel.field1 += 1
      finalVal = [txModel.field1, model.field2]
    })
    const updated = await txGet(key)
    expect(updated.field1).toBe(finalVal[0])
    expect(updated.field2).toBe(finalVal[1])
  }

  async testUpdateItemNonExisting () {
    const id = 'nonexist' + uuidv4()
    let fut = db.Transaction.run(async tx => {
      tx.update(TransactionModel,
        { id }, { field1: 2 })
    })
    await expect(fut).rejects.toThrow(Error)

    fut = db.Transaction.run(async tx => {
      tx.createOrPut(TransactionModel,
        { id },
        { field1: 3, field2: 1 })
    })
    await expect(fut).rejects.toThrow(db.InvalidParameterError)

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
    const key = TransactionModel.key(this.modelName)
    const origModel = await txGet(key)
    const newVal = Math.floor(Math.random() * 9999999)
    await db.Transaction.run(async tx => {
      const original = {}
      Object.keys(TransactionModel.__VIS_ATTRS).forEach(fieldName => {
        const val = origModel[fieldName]
        if (val !== undefined) {
          original[fieldName] = val
        }
      })
      tx.update(key.Cls, original, { field1: newVal })
    })
    const updated = await txGet(key)
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
    await expect(fut).rejects.toThrow(db.InvalidParameterError)

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
    await expect(fut).rejects.toThrow(db.InvalidFieldError)

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

    const model = await txGetRequired({ id: modelName }, (m) => {
      m.required = 1 // With required field, should work.
      m.field1 = 1
    })
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
      models.push(await tx.get(TransactionModel.key(uuidv4()),
        { createIfMissing: true }))
      const [model2, model3] = await tx.get([
        TransactionModel.key(uuidv4()),
        TransactionModel.key(uuidv4())
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

      await tx.__writeBatcher.__write(model1)
      checkModel(model1)

      model2.field1 = 0
      await tx.__writeBatcher.__write(model2)
      checkModel(model2)
    })
  }
}

runTests(
  ParameterTest,
  TransactionGetTest,
  TransactionWriteTest,
  TransactionRetryTest,
  TransactionBackoffTest,
  TransactionConditionCheckTest
)
