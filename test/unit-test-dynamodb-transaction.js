const uuidv4 = require('uuid').v4

const { BaseTest } = require('./base-unit-test')
const db = require('../src/dynamodb')()

async function txGet (key, id, func) {
  return db.Transaction.run(async tx => {
    const model = await tx.get(key, id, { createIfMissing: true })
    if (func) {
      func(model)
    }
    return model
  })
}

class TransactionModel extends db.Model {
  constructor (params) {
    super()
    this.params = params
    this.field1 = db.NumberField({ optional: true })
    this.field2 = db.NumberField({ optional: true })
    this.arrField = db.ArrayField({ optional: true })
    this.objField = db.ObjectField({ optional: true })
  }
}

class QuickTransactionTest extends BaseTest {
  mockTransactionDefaultOptions (options) {
    Object.defineProperty(db.Transaction.prototype, 'defaultOptions', {
      value: options,
      writable: false
    })
  }

  async setUp () {
    super.setUp()
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
      { maxBackoff: 199 }
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
  async setUp () {
    await super.setUp()
    await TransactionModel.createUnittestResource()
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
    const params = { something: { unique: 123321 }, createIfMissing: true }
    await db.Transaction.run(async (tx) => {
      const [m1, m2] = await tx.get([
        TransactionModel.key('a'),
        TransactionModel.key('b')
      ], params)
      expect(m1.params).toStrictEqual(params)
      expect(m2.params).toStrictEqual(params)

      const m3 = await tx.get(TransactionModel, 'c', params)
      const m4 = await tx.get(TransactionModel.key('d'), params)
      expect(m3.params).toStrictEqual(params)
      expect(m4.params).toStrictEqual(params)
    })
  }

  async testEventualConsistentGet () {
    const msg = uuidv4()
    const params = { consistentRead: true, createIfMissing: true }
    const originalFunc = TransactionModel.prototype.__getParams
    const mock = jest.fn().mockImplementation((ignore, params) => {
      expect(params.consistentRead).toBe(true)
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
    await TransactionModel.createUnittestResource()
    this.modelName = '1234'
    await txGet(TransactionModel, this.modelName, model => {
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
    const model = await txGet(key.Cls, key.compositeID)
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
    const model = await txGet(key.Cls, key.compositeID)
    expect(model.isNew).toBe(false)
    expect(model.field1).toBe(val)
  }

  async testCreateWithData () {
    const name = uuidv4()
    await db.Transaction.run(tx => {
      const model = tx.create(TransactionModel, { id: name, field1: 987 })
      model.field2 = 1
    })
    const model = await txGet(TransactionModel, name)
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
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    expect(tryCnt).toBe(1)
  }

  async testReadContention () {
    // When updating, if properties read in a transaction was updated outside,
    // contention!
    const key = TransactionModel.key(uuidv4())
    const fut = db.Transaction.run({ retries: 0 }, async (tx) => {
      const txModel = await tx.get(key, { createIfMissing: true })
      await txGet(key.Cls, key.compositeID, model => {
        model.field2 = 321
      })

      // Just reading a property that got changes outside of transaction
      // results in contention
      txModel.field2 // eslint-disable-line no-unused-expressions
      txModel.field1 = 123
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    const result = await txGet(key.Cls, key.compositeID)
    expect(result.field2).toBe(321)
  }

  async testWriteContention () {
    // When updating, if properties change in a transaction was also updated
    // outside, contention!
    let result
    const key = TransactionModel.key(this.modelName)
    const fut = db.Transaction.run({ retries: 0 }, async (tx) => {
      const txModel = await tx.get(key, { createIfMissing: true })
      await txGet(key.Cls, key.compositeID, model => {
        model.field2 += 1
        result = model.field2
      })

      txModel.field2 = 111
      txModel.field1 = 123
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
    const m = await txGet(key.Cls, key.compositeID)
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
    const updated = await txGet(key.Cls, key.compositeID)
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
      const model = await txGet(key.Cls, key.compositeID, model => {
        model.field2 += 1
      })

      txModel.field1 += 1
      finalVal = [txModel.field1, model.field2]
    })
    const updated = await txGet(key.Cls, key.compositeID)
    expect(updated.field1).toBe(finalVal[0])
    expect(updated.field2).toBe(finalVal[1])
  }
}

class TransactionRetryTest extends QuickTransactionTest {
  async expectRetries (err, maxTries, expectedRuns) {
    let cnt = 0
    const fut = db.Transaction.run({ retries: maxTries }, () => {
      cnt++
      throw err
    })
    await expect(fut).rejects.toThrow(db.TransactionFailedError)
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

const tests = [
  ParameterTest,
  TransactionGetTest,
  TransactionWriteTest,
  TransactionRetryTest,
  TransactionBackoffTest,
  TransactionConditionCheckTest
]
tests.forEach(test => test.runTests())
