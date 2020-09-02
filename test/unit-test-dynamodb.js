const { BaseServiceTest, runTests } = require('./base-unit-test')
const db = require('../src/dynamodb')
const PropData = require('../src/sharedlib-apis-dynamodb').models[1]

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
    expect(PropData.name).toBe('PropData') // check the import is correct
    async function getItem () {
      return db.Transaction.run(tx => {
        return tx.get(PropData, 'a:1',
          { createIfMissing: true, propCount: 1 })
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

runTests(DynamodbLibTest)
