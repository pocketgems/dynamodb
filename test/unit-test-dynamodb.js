const uuidv4 = require('uuid').v4

const db = require('../src/dynamodb')
const S = require('../src/schema')
const { PropDataModels } = require('../src/sharedlib-apis-dynamodb')

const { BaseServiceTest, BaseTest, runTests } = require('./base-unit-test')

function getURI (postfix) {
  return '/internal/sharedlib' + postfix
}

class DynamodbLibTest extends BaseServiceTest {
  async testPropModelWorks () {
    const app = this.app
    // invalid body format
    await app.post(getURI('/proptest'))
      .set('Content-Type', 'application/json')
      .send({
        modelNamePrefix: 'unittest',
        propCount: 3,
        readPropCount: 3,
        writePropCount: 3
      })
      .expect(200)
  }

  async testWriteItem () {
    const PropData1 = PropDataModels['1']
    async function getItem () {
      return db.Transaction.run(tx => {
        return tx.get(PropData1, 'a:1', { createIfMissing: true })
      })
    }
    const oldModel = await getItem()
    await this.app.post(getURI('/proptest'))
      .set('Content-Type', 'application/json')
      .send({
        modelNamePrefix: 'a',
        propCount: 1,
        readPropCount: 1,
        writePropCount: 1
      })
      .expect(200)
    const newModel = await getItem()
    expect(newModel.prop0).toBe(oldModel.prop0 + 1)
  }

  async testThrow500 () {
    // Make sure custom loggers etc works.
    const result = await this.app.post(getURI('/throw500')).expect(500)
    expect(result.body.stack.join('\n')).toContain('/sharedlib')
  }

  async testClientErrorAPIWorking () {
    return this.app.post(getURI('/clienterrors'))
      .send('{"json": {"anything": ["goes"]}}')
      .set('Content-Type', 'application/json')
      .expect(200)
  }

  async testQueryJsonFail () {
    const result = await this.app.post(getURI('/clienterrors'))
      .query('{d}').expect(400)
    expect(result.body.errObj.name).toBe('Body Validation Failure')
  }

  async testBodyJsonFail () {
    const result = await this.app.post(getURI('/clienterrors'))
      .send('{d}')
      .set('Content-Type', 'application/json')
      .expect(400)
    expect(result.body.errObj.name).toBe('Body Parse Failure')
  }

  async testMissingRequiredPropFail () {
    const result = await this.app.post(getURI('/clienterrors'))
      .send('{}')
      .set('Content-Type', 'application/json')
      .expect(400)
    expect(result.body.errObj.name).toBe('Body Validation Failure')
  }

  async testBodyContentTypeFail () {
    const result = await this.app.post(getURI('/clienterrors'))
      .send('{}')
      .set('Content-Type', 'text/html')
      .expect(415)
    expect(result.body.errObj.name).toBe('Content-Type Not Permitted')
  }

  async testValidJsonSchema () {
    await this.app.post(getURI('/jsonschema'))
      .set('Content-Type', 'application/json')
      .send({
        modelCount: 1
      })
      .expect(200)
  }

  async testTxAPICommit () {
    const maxRetriesToSucceed = 3
    const nValues = {}
    const app = this.app
    async function check (id, delta, numTimesToRetry, failInPreCommit) {
      const shouldSucceed = numTimesToRetry <= maxRetriesToSucceed
      const resp = await app.post(getURI('/dbWithTxAPI'))
        .set('Content-Type', 'application/json')
        .send({ id, delta, numTimesToRetry, failInPreCommit })
        .expect(shouldSucceed ? 200 : 500)

      if (!shouldSucceed) {
        expect(resp.body.errObj.name).toBe('TransactionFailedError')
        return
      }

      if (!nValues[id]) {
        nValues[id] = 0
      }
      nValues[id] += 5 + delta
      expect(resp.body).toEqual({
        computeCalls: numTimesToRetry + 1,
        postComputeCalls: failInPreCommit ? (numTimesToRetry + 1) : 1,
        n: nValues[id],
        postCommitMsg: 'commit succeeded'
      })
    }
    const id1 = uuidv4()
    await check(id1, 3, 2, false) // try failing in computeResponse()
    await check(id1, 4, 3, true) // try failing in postOkResponse()
    await check(id1, 7, 0, true) // can add more
    await check(uuidv4(), -1, 0, true) // can work on other items too
    await check(id1, 2, 4, false) // can fail all retries => no postCommit()
    await check(id1, 1, 0, false) // check value is okay after failure
  }

  async testRememberTooMuch () {
    const app = this.app
    let lifetimeTries = 0
    async function check (numTries) {
      const resp = await app.post(getURI('/overshare'))
        .set('Content-Type', 'application/json')
        .send({ numTries })
        .expect(200)
      lifetimeTries += numTries
      expect(resp.body).toEqual({
        numTries,
        numTriesOnThisMachine: lifetimeTries
      })
    }
    await check(1)
    await check(3)
    await check(2)
    expect(lifetimeTries).toBe(6)
  }
}

class Order extends db.Model {
  static FIELDS = {
    product: S.str,
    quantity: S.int
  }
}

class OrderWithPrice extends db.Model {
  static FIELDS = {
    quantity: S.int,
    unitPrice: S.int.desc('price per unit in cents')
  }

  totalPrice (salesTax = 0.1) {
    const subTotal = this.quantity * this.unitPrice
    return subTotal * (1 + salesTax)
  }
}

class RaceResult extends db.Model {
  static KEY = {
    raceID: S.int,
    runnerName: S.str
  }
}

class ModelWithFields extends db.Model {
  static FIELDS = {
    someInt: S.int.min(0),
    someBool: S.bool,
    someObj: S.obj().prop('arr', S.arr(S.str))
  }
}

class ModelWithComplexFields extends db.Model {
  static FIELDS = {
    aNonNegInt: S.int.min(0),
    anOptBool: S.bool.optional(), // default value is undefined
    // this field defaults to 5; once it is set, it cannot be changed (though
    // it won't always be 5 since it can be created with a non-default value)
    immutableInt: S.int.readOnly().default(5)
  }
}

// code from the readme (this suite is not intended to create comprehensive
// tests for features; it only verifies that code from the readme actually runs
// correctly (and continues to do so after any library changes)
class DBReadmeTest extends BaseTest {
  async beforeAll () {
    await Order.createUnittestResource()
    await RaceResult.createUnittestResource()
    await ModelWithFields.createUnittestResource()
    await ModelWithComplexFields.createUnittestResource()
  }

  async testMinimalExample () {
    await Order.createUnittestResource()
    const id = uuidv4()
    await db.Transaction.run(tx => {
      const order = tx.create(Order, { id, product: 'coffee', quantity: 1 })
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(1)
    })
    // Example
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, id)
      expect(order.id).toBe(id)
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(1)
      order.quantity = 2
    })
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, id)
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(2)
    })
  }

  async testKeys () {
    await RaceResult.createUnittestResource()
    await db.Transaction.run(async tx => {
      const raceResult = await tx.get(
        RaceResult,
        { raceID: 99, runnerName: 'Bo' },
        { createIfMissing: true })
      expect(raceResult.raceID).toBe(99)
      expect(raceResult.runnerName).toBe('Bo')
    })
  }

  async testFields () {
    await ModelWithComplexFields.createUnittestResource()
    async function _check (how, expErr, values) {
      const id = uuidv4()
      const expBool = values.anOptBool
      const expNNInt = values.aNonNegInt
      const givenImmInt = values.immutableInt
      const expImmutableInt = (givenImmInt === undefined) ? 5 : givenImmInt

      function checkItem (item) {
        expect(item.id).toBe(id)
        expect(item.anOptBool).toBe(expBool)
        expect(item.aNonNegInt).toBe(expNNInt)
        expect(item.immutableInt).toBe(expImmutableInt)
      }

      const ret = db.Transaction.run(async tx => {
        const data = { id, ...values }
        let item
        if (how === 'create') {
          item = tx.create(ModelWithComplexFields, data)
        } else {
          item = await tx.get(
            ModelWithComplexFields, data, { createIfMissing: true })
          expect(item.isNew).toBe(true)
        }
        checkItem(item)
      })
      if (expErr) {
        await expect(ret).rejects.toThrow(expErr)
      } else {
        await ret
        await db.Transaction.run(async tx => {
          const item = await tx.get(ModelWithComplexFields, id)
          checkItem(item)
          expect(() => { item.immutableInt = 5 }).toThrow(/is immutable/)
        })
      }
    }
    async function check (values, expErr) {
      await _check('create', expErr, values)
      await _check('get', expErr, values)
    }
    // override the default value for the immutable int
    await check({ anOptBool: true, aNonNegInt: 0, immutableInt: 0 })
    // it's an error to try to set a required field to undefined (explicitly
    // passing undefined overrides the default)
    await check({ anOptBool: false, aNonNegInt: 1, immutableInt: undefined },
      /immutableInt missing required value/)
    // can omit a field with a default and it will be populated
    await check({ anOptBool: false, aNonNegInt: 2 })
    // can explicitly set an optional field to undefined
    await check({ anOptBool: undefined, aNonNegInt: 3 })
    // can also omit an optional field altogether
    await check({ aNonNegInt: 4 })
    // schemas still have to be met
    await check({ aNonNegInt: -5 },
      'Validation Error: ModelWithComplexFields.aNonNegInt')
    await check({ aNonNegInt: 6, anOptBool: 'true' },
      'Validation Error: ModelWithComplexFields.anOptBool')
    await check({ aNonNegInt: 7, immutableInt: '5' },
      'Validation Error: ModelWithComplexFields.immutableInt')

    // this is the portion from the readme; the earlier part of this test is
    // thoroughly checking correctness
    await db.Transaction.run(async tx => {
      // example1122start
      // can omit the optional field
      const item = tx.create(ModelWithComplexFields, {
        id: uuidv4(),
        aNonNegInt: 0,
        immutableInt: 3
      })
      expect(item.aNonNegInt).toBe(0)
      // omitted optional field => undefined
      expect(item.anOptBool).toBe(undefined)
      expect(item.immutableInt).toBe(3)

      // can override the default value
      const item2 = tx.create(ModelWithComplexFields, {
        id: uuidv4(),
        aNonNegInt: 1,
        anOptBool: true
      })
      expect(item2.aNonNegInt).toBe(1)
      expect(item2.anOptBool).toBe(true)
      expect(item2.immutableInt).toBe(5) // the default value
      // can't change read only fields:
      expect(() => { item2.immutableInt = 3 }).toThrow(
        'immutableInt is immutable so value cannot be changed')
      // example1122end
    })
  }

  async testSchemaEnforcement () {
    await ModelWithFields.createUnittestResource()
    const id = uuidv4()
    await db.Transaction.run(tx => {
      // fields are checked immediately when creating a new item; this throws
      // db.InvalidFieldError because someInt should be an integer
      const data = {
        id,
        someInt: '1',
        someBool: true,
        someObj: { arr: [] }
      }
      expect(() => {
        tx.create(ModelWithFields, data)
      }).toThrow(S.ValidationError)
      data.someInt = 1
      const x = tx.create(ModelWithFields, data)

      // fields are checked when set
      expect(() => {
        x.someBool = 1 // throws because the type should be boolean not int
      }).toThrow(S.ValidationError)
      expect(() => {
        x.someObj = {} // throws because the required "arr" key is missing
      }).toThrow(S.ValidationError)
      expect(() => {
        // throws b/c arr is supposed to contain strings
        x.someObj = { arr: [5] }
      }).toThrow(S.ValidationError)
      x.someObj = { arr: ['ok'] } // ok!
    })

    const badTx = db.Transaction.run(async tx => {
      const item = await tx.get(ModelWithFields, id)
      expect(item.someInt).toBe(1)
      expect(item.someBool).toBe(true)
      expect(item.someObj).toEqual({ arr: ['ok'] })
      // changes within a non-primitive type aren't detected or validated until
      // we try to write the change so this next line won't throw!
      item.someObj.arr.push(5)

      expect(() => {
        item.getField('someObj').validate()
      }).toThrow(S.ValidationError)
    })
    await expect(badTx).rejects.toThrow(S.ValidationError)

    // compound key validation
    async function check (compoundID, isOk) {
      const funcs = [
        // each of these three trigger a validation check (to verify that
        // compoundID contains every key component and that each of them meet
        // their respective schemas requirements)
        () => RaceResult.key(compoundID),
        tx => tx.create(RaceResult, compoundID),
        async tx => { await tx.get(RaceResult, compoundID) }
      ]
      funcs.forEach(async func => {
        await db.Transaction.run(async tx => {
          if (isOk) {
            await func(tx)
          } else {
            await expect(async () => func(tx)).rejects.toThrow()
          }
        })
      })
    }
    const runnerName = uuidv4()
    await check({ raceID: '1', runnerName }, false)
    await check({ raceID: 1, runnerName }, true)
  }

  async testCustomMethods () {
    await OrderWithPrice.createUnittestResource()
    await db.Transaction.run(tx => {
      const id = uuidv4()
      const order = tx.create(OrderWithPrice, {
        id,
        quantity: 2,
        unitPrice: 200
      })
      expect(order.totalPrice()).toBeCloseTo(440)
    })
  }

  async testGuestbook () {
    class Guestbook extends db.Model {
      static FIELDS = { names: S.arr(S.str) }
    }
    await Guestbook.createUnittestResource()
    const id = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(Guestbook, { id, names: [] })
    })
    async function addName (name) {
      return db.Transaction.run(async tx => {
        const gb = await tx.get(Guestbook, id)
        gb.names.push(name)
        return gb
      })
    }
    let [gb1, gb2] = await Promise.all([addName('Alice'), addName('Bob')])
    if (gb2.names.length === 1) {
      // store first one to complete in gb1 to simplify code below
      [gb1, gb2] = [gb2, gb1]
    }
    expect(gb1.names.length + gb2.names.length).toBe(3)
    expect(gb1.names.length).toBe(1)
    expect(['Alice', 'Bob']).toContain(gb1.names[0])
    gb2.names.sort()
    expect(gb2.names).toEqual(['Alice', 'Bob'])
  }

  async testTxRetries () {
    const retryOptions = {
      retries: 4, // 1 initial run + up to 4 retry attempts = max 5 total attempts
      initialBackoff: 1, // 1 millisecond (+/- a small random offset)
      maxBackoff: 200 // no more than 200 milliseconds
    }
    let count = 0
    await expect(db.Transaction.run(retryOptions, async tx => {
      // you can also manually force your transaction to retry by throwing a
      // custom exception with the "retryable" property set to true
      count += 1
      const error = new Error()
      error.retryable = true
      throw error
    })).rejects.toThrow('Too much contention')
    expect(count).toBe(5)
  }

  async testRaceCondition () {
    class SkierStats extends db.Model {
      static KEY = { resort: S.str }
      static FIELDS = { numSkiers: S.int.min(0).default(0) }
    }
    await SkierStats.createUnittestResource()
    class LiftStats extends db.Model {
      static KEY = { resort: S.str }
      static FIELDS = { numLiftRides: S.int.min(0).default(0) }
    }
    await LiftStats.createUnittestResource()

    async function liftRideTaken (resort, isNewSkier) {
      await db.Transaction.run(async tx => {
        const opts = { createIfMissing: true }
        const [skierStats, liftStats] = await Promise.all([
          !isNewSkier ? Promise.resolve() : tx.get(SkierStats, resort, opts),
          tx.get(LiftStats, resort, opts)])
        if (isNewSkier) {
          skierStats.numSkiers += 1
        }
        liftStats.numLiftRides += 1
      })
    }

    // force the skier stats fetch to resolve first
    const resort = uuidv4()
    await db.Transaction.run(async tx => {
      const skierStats = await tx.get(SkierStats, resort)
      await liftRideTaken(resort, true)
      const liftStats = await tx.get(LiftStats, resort)
      expect(skierStats).toEqual(undefined)
      expect(liftStats.numLiftRides).toEqual(1)
    })

    await db.Transaction.run(async tx => {
      const [skierStats, liftStats] = await tx.get([
        SkierStats.key(resort),
        LiftStats.key(resort)
      ])
      expect(skierStats.numSkiers).toEqual(1)
      expect(liftStats.numLiftRides).toEqual(1)
    })
  }

  async testAddressingItems () {
    const id = uuidv4()
    expect(Order.key({ id }).keyComponents.id).toBe(id)
    expect(Order.key(id).keyComponents.id).toBe(id)

    await db.Transaction.run(async tx => {
      tx.create(Order, { id, product: 'coffee', quantity: 1 })
    })
    async function check (...args) {
      await db.Transaction.run(async tx => {
        const item = await tx.get(...args)
        expect(item.id).toBe(id)
        expect(item.product).toBe('coffee')
        expect(item.quantity).toBe(1)
      })
    }
    await check(Order.key(id))
    await check(Order, id)
    await check(Order.key({ id }))
    await check(Order, { id })
  }

  async testAddressingCompoundItems () {
    const raceID = 20140421
    const runnerName = 'Meb'
    const kc = RaceResult.key({ raceID, runnerName }).keyComponents
    expect(kc.raceID).toBe(raceID)
    expect(kc.runnerName).toBe(runnerName)
    await db.Transaction.run(async tx => {
      const item = await tx.get(RaceResult, { raceID, runnerName },
        { createIfMissing: true })
      expect(item.raceID).toBe(raceID)
      expect(item.runnerName).toBe(runnerName)
    })
  }

  async testCreateIfMissing () {
    const id = uuidv4()
    const dataIfOrderIsNew = { id, product: 'coffee', quantity: 1 }
    async function getAndCreateIfMissing () {
      return db.Transaction.run(async tx => {
        const order = await tx.get(Order, dataIfOrderIsNew,
          { createIfMissing: true })
        return order.isNew
      })
    }
    expect(await getAndCreateIfMissing()).toBe(true) // missing; so create it
    expect(await getAndCreateIfMissing()).toBe(false) // already exists by now
  }

  async testReadConsistency () {
    const data = { id: uuidv4(), product: 'coffee', quantity: 1 }
    await db.Transaction.run(tx => tx.create(Order, data))
    const item = await db.Transaction.run(async tx => tx.get(
      Order, data.id, { inconsistentRead: true }))
    expect(item.id).toEqual(data.id)
    expect(item.product).toEqual(data.product)
    expect(item.quantity).toEqual(data.quantity)
  }

  async testBatchRead () {
    const id = uuidv4()
    const id2 = uuidv4()
    const raceID = 123
    const runnerName = uuidv4()
    function check (order1, order2, raceResult) {
      expect(order1.id).toBe(id)
      expect(order1.product).toBe('coffee')
      expect(order1.quantity).toBe(1)
      expect(order2.id).toBe(id2)
      expect(order2.product).toBe('spoon')
      expect(order2.quantity).toBe(10)
      expect(raceResult.raceID).toBe(raceID)
      expect(raceResult.runnerName).toBe(runnerName)
    }

    await db.Transaction.run(async tx => {
      const [order1, order2, raceResult] = await tx.get([
        Order.data({ id, product: 'coffee', quantity: 1 }),
        Order.data({ id: id2, product: 'spoon', quantity: 10 }),
        RaceResult.data({ raceID, runnerName })
      ], { createIfMissing: true })
      check(order1, order2, raceResult)
    })

    await db.Transaction.run(async tx => {
      const [order1, order2, raceResult] = await tx.get([
        Order.key(id),
        Order.key(id2),
        RaceResult.key({ raceID, runnerName })
      ])
      check(order1, order2, raceResult)
    })
  }

  async testBlindWritesUpdate () {
    const id = uuidv4()
    const data = { id, product: 'coffee', quantity: 1 }
    await db.Transaction.run(tx => tx.create(Order, data))
    await db.Transaction.run(async tx => {
      const ret = tx.update(
        Order, { id, quantity: 1, product: 'coffee' }, { quantity: 2 })
      expect(ret).toBe(undefined) // should not return anything
    })
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, id)
      expect(order.id).toBe(id)
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(2)
    })
  }

  async testBlindWritesCreateOrUpdate () {
    class LastUsedFeature extends db.Model {
      static KEY = {
        user: S.str,
        feature: S.str
      }

      static FIELDS = { epoch: S.int }
    }
    await LastUsedFeature.createUnittestResource()
    await db.Transaction.run(tx => {
      const ret = tx.createOrPut(LastUsedFeature,
        // these are the values we expect (must include all key components);
        // this call fails if the data exists AND it doesn't match these values
        { user: 'Bob', feature: 'refer a friend' },
        // this contains the new value(s); if a value is undefined then the
        // field will be deleted (it must be optional for this to be allowed)
        { epoch: 123 })
      expect(ret).toBe(undefined) // should not return anything
    })
    await db.Transaction.run(async tx => {
      const item = await tx.get(LastUsedFeature,
        { user: 'Bob', feature: 'refer a friend' })
      expect(item.epoch).toBe(123)
    })
  }

  async testCreateViaGetAndIncrement () {
    const id = uuidv4()
    await db.Transaction.run(async tx => {
      const x = await tx.get(
        Order.data({ id, product: 'coffee', quantity: 9 }),
        { createIfMissing: true })
      x.getField('quantity').incrementBy(1)
      // access value through field so we don't mess with the __Field's state
      expect(x.getField('quantity').__value).toBe(10)
    })
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, id)
      expect(order.quantity).toBe(10)
    })
  }

  async testCreateAndIncrement () {
    const id = uuidv4()
    await db.Transaction.run(async tx => {
      const x = tx.create(Order, { id, product: 'coffee', quantity: 9 })
      x.getField('quantity').incrementBy(1)
      // access value through field so we don't mess with the __Field's state
      expect(x.getField('quantity').__value).toBe(10)
    })
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, id)
      expect(order.quantity).toBe(10)
    })
  }

  async testUpdatingItemWithoutAOL () {
    const id = uuidv4()
    await db.Transaction.run(async tx => {
      tx.create(Order, { id, product: 'coffee', quantity: 8 })
    })
    async function incrUpToButNotBeyondTen (origValue) {
      await db.Transaction.run(async tx => {
        const x = await tx.get(Order, id)
        if (x.quantity < 10) {
          x.getField('quantity').incrementBy(1)
          // trying to modify the quantity directly would generate a condition
          // on the old value (e.e.g, "set quantity to 9 if it was 8") which is
          // less scalable than "increment quantity by 1".
          // x.quantity += 1
        }
        const [cond, vals] = x.getField('quantity').__conditionExpression(':_1')
        expect(vals).toEqual({ ':_1': origValue })
        expect(cond).toBe('#quantity=:_1')
      })
      await db.Transaction.run(async tx => {
        const order = await tx.get(Order, id)
        expect(order.quantity).toBe(Math.min(origValue + 1, 10))
      })
    }
    await incrUpToButNotBeyondTen(8) // goes up by one
    await incrUpToButNotBeyondTen(9) // goes up again
    await incrUpToButNotBeyondTen(10) // but not any further
    await incrUpToButNotBeyondTen(10) // no matter how many times we use it
  }

  async testKeyEncoding () {
    const err = new Error('do not want to save this')
    await expect(db.Transaction.run(tx => {
      const item = tx.create(RaceResult, { raceID: 123, runnerName: 'Joe' })
      expect(item._id).toBe('123\0Joe')
      throw err // don't want to save this to the test db
    })).rejects.toThrow(err)

    const key = RaceResult.key({ runnerName: 'Mel', raceID: 123 })
    expect(key.Cls).toBe(RaceResult)
    expect(key.encodedKeys._id).toBe('123\0Mel')

    class StringKeyWithNullBytes extends db.Model {
      static KEY = { id: S.obj().prop('raw', S.str) }
    }
    await StringKeyWithNullBytes.createUnittestResource()
    const strWithNullByte = 'I can contain \0, no pr\0blem!'
    await expect(db.Transaction.run(tx => {
      const item = tx.create(StringKeyWithNullBytes, {
        id: {
          raw: strWithNullByte
        }
      })
      expect(item.id.raw).toBe(strWithNullByte)
      throw err // don't want to save this to the test db
    })).rejects.toThrow(err)
  }

  async testSortKeys () {
    class CustomerData extends db.Model {
      static KEY = { store: S.str }
      static SORT_KEY = { customer: S.str }
    }
    await CustomerData.createUnittestResource()
    await db.Transaction.run(tx => {
      tx.create(CustomerData, { store: 'Wallymart', customer: uuidv4() })
    })
  }

  async testOverlappingModels () {
    class Inventory extends db.Model {
      // we override the default table name so that our subclasses all use the same
      // table name
      static tableName = 'Inventory'
      static KEY = { userID: S.str }
      static get SORT_KEY () {
        return { typeKey: S.str.default(this.INVENTORY_ITEM_TYPE) }
      }

      static get FIELDS () {
        return {
          stuff: S.obj({
            usd: S.int.optional(),
            rmb: S.int.optional(),
            ax: S.obj().optional()
          }).default({}).optional()
        }
      }

      static INVENTORY_ITEM_TYPE () { throw new Error('To be overwritten') }
    }
    class Currency extends Inventory {
      static INVENTORY_ITEM_TYPE = 'money'
    }
    await Currency.createUnittestResource()
    class Weapon extends Inventory {
      static INVENTORY_ITEM_TYPE = 'weapon'
      static FIELDS = {
        ...super.FIELDS,
        weaponSkillLevel: S.int
      }
    }
    await Weapon.createUnittestResource()

    // both items will be stored in the Inventory; both will also be stored on
    // the same database node since they share the same partition key (userID)
    const userID = uuidv4()
    await db.Transaction.run(tx => {
      tx.create(Currency, {
        userID,
        typeKey: Currency.INVENTORY_ITEM_TYPE,
        stuff: { usd: 123, rmb: 456 }
      })
      tx.create(Weapon, {
        userID,
        typeKey: Weapon.INVENTORY_ITEM_TYPE,
        stuff: { ax: {/* ... */} },
        weaponSkillLevel: 13
      })
    })
  }

  async testIncrementBy () {
    class WebsiteHitCounter extends db.Model {
      static FIELDS = { count: S.int.min(0) }
    }
    await WebsiteHitCounter.createUnittestResource()

    async function slowlyIncrement (id) {
      return db.Transaction.run(async tx => {
        const counter = await tx.get(WebsiteHitCounter, id)
        // here we read and write the data, so the library will generate an
        // update like "if count was N then set count to N + 1"
        counter.count += 1
        expect(counter.getField('count').canUpdateWithoutCondition).toBe(false)
      })
    }

    async function quicklyIncrement (id) {
      return db.Transaction.run(async tx => {
        const counter = await tx.get(WebsiteHitCounter, id)
        // since we only increment the number and never read it, the library
        // will generate an update like "increment quantity by 1" which will
        // succeed no matter what the original value was
        counter.getField('count').incrementBy(1)
        expect(counter.getField('count').canUpdateWithoutCondition).toBe(true)
      })
    }

    async function bothAreJustAsFast (id) {
      return db.Transaction.run(async tx => {
        const counter = await tx.get(WebsiteHitCounter, id)
        if (counter.count < 100) { // stop counting after reaching 100
          // this is preferred here b/c it is simpler and just as fast in this case
          // counter.count += 1

          // isn't any faster because we have to generate the condition
          // expression due to the above if condition which read the count var
          counter.getField('count').incrementBy(1)

          expect(counter.getField('count').canUpdateWithoutCondition).toBe(
            false)
        }
      })
    }

    async function checkVal (id, expVal) {
      await db.Transaction.run(async tx => {
        const counter = await tx.get(WebsiteHitCounter, id)
        expect(counter.count).toBe(expVal)
      })
    }

    const id = uuidv4()
    await db.Transaction.run(tx => tx.create(
      WebsiteHitCounter, { id, count: 0 }))
    await slowlyIncrement(id)
    await checkVal(id, 1)
    await slowlyIncrement(id)
    await checkVal(id, 2)
    await quicklyIncrement(id)
    await checkVal(id, 3)
    await quicklyIncrement(id)
    await checkVal(id, 4)
    await bothAreJustAsFast(id)
    await checkVal(id, 5)
  }
}

runTests(DynamodbLibTest, DBReadmeTest)
