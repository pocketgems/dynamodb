const S = require('fluent-schema')
const uuidv4 = require('uuid').v4

const { BaseServiceTest, BaseTest, runTests } = require('./base-unit-test')
const db = require('../src/dynamodb')
const { PropDataModels } = require('../src/sharedlib-apis-dynamodb')

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
      .send('{}')
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

class ModelWithOptionalFields extends db.Model {
  static FIELDS = {
    someNumber: S.integer().minimum(0),
    someBool: S.boolean().optional(),
    anotherNum: S.integer().readOnly().default(5)
  }
}

// code from the readme
class ReadmeTest extends BaseTest {
  async setUp () {
    await Order.createUnittestResource()
    await RaceResult.createUnittestResource()
    await ModelWithFields.createUnittestResource()
    await ModelWithOptionalFields.createUnittestResource()
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

  async testSchemaEnforcement () {
    const id = uuidv4()
    await db.Transaction.run(tx => {
      // fields are checked immediately when creating a new item; this throws
      // db.InvalidFieldError because someNumber should be an integer
      expect(() => {
        tx.create(ModelWithOptionalFields, { id, someNumber: '1' })
      }).toThrow(db.InvalidFieldError)
      tx.create(ModelWithOptionalFields, { id, someNumber: 1 })

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
        tx.get(ModelWithOptionalFields.key(id)),
        tx.get(ModelWithFields.key(id))
      ])
      expect(opt.someNumber).toBe(1)
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
}

runTests(DynamodbLibTest, ReadmeTest)
