const S = require('fluent-schema')
const uuidv4 = require('uuid').v4

const db = require('../src/dynamodb')
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
    expect(result.body.error.name).toBe('Body Validation Failure')
  }

  async testBodyJsonFail () {
    const result = await this.app.post(getURI('/clienterrors'))
      .send('{d}')
      .set('Content-Type', 'application/json')
      .expect(400)
    expect(result.body.error.name).toBe('Body Parse Failure')
  }

  async testMissingRequiredPropFail () {
    const result = await this.app.post(getURI('/clienterrors'))
      .send('{}')
      .set('Content-Type', 'application/json')
      .expect(400)
    expect(result.body.error.name).toBe('Body Validation Failure')
  }

  async testBodyContentTypeFail () {
    const result = await this.app.post(getURI('/clienterrors'))
      .send('{}')
      .set('Content-Type', 'text/html')
      .expect(415)
    expect(result.body.error.name).toBe('Content-Type Not Permitted')
  }

  async testValidJsonSchema () {
    await this.app.post(getURI('/jsonschema'))
      .set('Content-Type', 'application/json')
      .send({
        modelCount: 1
      })
      .expect(200)
  }

  async testTxAPIommit () {
    const maxRetriesToSucceed = 3
    const nValues = {}
    const app = this.app
    async function check (id, delta, numTimesToRetry, failInPostCompute) {
      const shouldSucceed = numTimesToRetry <= maxRetriesToSucceed
      const resp = await app.post(getURI('/dbWithTxAPI'))
        .set('Content-Type', 'application/json')
        .send({ id, delta, numTimesToRetry, failInPostCompute })
        .expect(shouldSucceed ? 200 : 500)

      if (!shouldSucceed) {
        expect(resp.body.error.name).toBe('TransactionFailedError')
        return
      }

      if (!nValues[id]) {
        nValues[id] = 0
      }
      nValues[id] += 5 + delta
      expect(resp.body).toEqual({
        computeCalls: numTimesToRetry + 1,
        postComputeCalls: failInPostCompute ? (numTimesToRetry + 1) : 1,
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
    product: S.string(),
    quantity: S.integer()
  }
}

class RaceResult extends db.Model {
  static KEY = {
    raceID: S.integer(),
    runnerName: S.string()
  }
}

class ModelWithFields extends db.Model {
  static FIELDS = {
    someNumber: S.integer().minimum(0),
    someBool: S.boolean(),
    someObj: S.object().prop('arr', S.array().items(S.string()))
  }
}

class ModelWithComplexFields extends db.Model {
  static FIELDS = {
    aNonNegInt: S.integer().minimum(0),
    anOptBool: S.boolean().optional(), // default value is undefined
    // this field defaults to 5; once it is set, it cannot be changed (though
    // it won't always be 5 since it can be created with a non-default value)
    immutableInt: S.integer().readOnly().default(5)
  }
}

// code from the readme
class DBReadmeTest extends BaseTest {
  async setUp () {
    await Order.createUnittestResource()
    await RaceResult.createUnittestResource()
    await ModelWithFields.createUnittestResource()
    await ModelWithComplexFields.createUnittestResource()
  }

  async testMinimalExample () {
    const id = uuidv4()
    await db.Transaction.run(tx => {
      const order = tx.create(Order, { id, product: 'coffee', quantity: 1 })
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(1)
    })
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, { id })
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(1)
      order.quantity = 2
    })
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, { id })
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(2)
    })
  }

  async testKeys () {
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
    await check({ aNonNegInt: -5 }, /aNonNegInt.*does not conform/)
    await check({ aNonNegInt: 6, anOptBool: 'true' }, /anOptBool.*not conform/)
    await check({ aNonNegInt: 7, immutableInt: '5' },
      /immutableInt.*does not conform/)
  }

  async testSchemaEnforcement () {
    const id = uuidv4()
    await db.Transaction.run(tx => {
      // fields are checked immediately when creating a new item; this throws
      // db.InvalidFieldError because someNumber should be an integer
      expect(() => {
        tx.create(ModelWithComplexFields, { id, aNonNegInt: '1' })
      }).toThrow(db.InvalidFieldError)
      tx.create(ModelWithComplexFields, { id, aNonNegInt: 1 })

      // fields are checked when set
      const x = tx.create(ModelWithFields, {
        id, someNumber: 1, someBool: false, someObj: { arr: [] }
      })
      expect(() => {
        x.someBool = 1 // throws because the type should be boolean not int
      }).toThrow(db.InvalidFieldError)
      expect(() => {
        x.someObj = {} // throws because the required "arr" key is missing
      }).toThrow(db.InvalidFieldError)
      expect(() => {
        // throws b/c arr is supposed to contain strings
        x.someObj = { arr: [5] }
      }).toThrow(db.InvalidFieldError)
      x.someObj = { arr: ['ok'] } // ok!
    })

    const badTx = db.Transaction.run(async tx => {
      const [opt, req] = await Promise.all([
        tx.get(ModelWithComplexFields.key(id)),
        tx.get(ModelWithFields.key(id))
      ])
      expect(opt.aNonNegInt).toBe(1)
      expect(req.someNumber).toBe(1)
      expect(req.someBool).toBe(false)
      expect(req.someObj).toEqual({ arr: ['ok'] })
      // changes within a non-primitive type aren't detected or validated until
      // we try to write the change so this next line won't throw!
      req.someObj.arr.push(5)

      expect(() => {
        req.getField('someObj').validate()
      }).toThrow(db.InvalidFieldError)
    })
    await expect(badTx).rejects.toThrow(db.InvalidFieldError)
  }

  async testKeyEncoding () {
    const key = RaceResult.key({ runnerName: 'Mel', raceID: 123 })
    expect(key.Cls).toBe(RaceResult)
    expect(key.encodedKeys._id).toBe('123\0Mel')
  }
}

runTests(DynamodbLibTest, DBReadmeTest)
