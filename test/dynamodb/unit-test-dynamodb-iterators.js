const uuidv4 = require('uuid').v4

const S = require('../../src/schema/src/schema')
const { BaseTest, runTests } = require('../base-unit-test')
const db = require('../db-with-field-maker')

const {
  Query,
  Scan
} = db.__private

class TestIteratorModel extends db.Model {
  static KEY = {
    id1: S.str,
    id2: S.int
  }

  static SORT_KEY = {
    sk1: S.str,
    sk2: S.str
  }

  static FIELDS = {
    field1: S.str,
    field2: S.str,
    field3: S.arr(S.obj({ a: S.int })).optional()
  }

  static INDEXES = {
    index1: { KEY: ['id1', 'id2'], SORT_KEY: ['field1'] },
    index2: { KEY: ['id1', 'sk1'], SORT_KEY: ['field1', 'field2'] },
    index3: { KEY: ['field3'] }
  }
}

class IteratorTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await TestIteratorModel.createResource()
  }

  async testConstructorInputValidation () {
    expect(() => {
      // eslint-disable-next-line no-new
      new Query({ options: { abc: 123 } })
    }).toThrow(/Invalid option value for abc/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Scan({ options: { descending: 123 } })
    }).toThrow(/Invalid option value for descending/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Query({ options: { shardCount: 123 } })
    }).toThrow(/Invalid option value for shardCount/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Query({ options: { shardIndex: 123 } })
    }).toThrow(/Invalid option value for shardIndex/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Scan({ options: { shardIndex: 123 } })
    }).toThrow(/Invalid option value for shardIndex & shardCount/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Scan({ options: { allowLazyFilter: true } })
    }).toThrow(/Invalid option value for allowLazyFilter/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Scan({ ModelCls: TestIteratorModel, options: { index: 'invalidIndex' } })
    }).toThrow(/Invalid option value for index invalidIndex/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Query({ ModelCls: TestIteratorModel, options: { index: 'invalidIndex' } })
    }).toThrow(/Invalid option value for index invalidIndex/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Query({
        ModelCls: TestIteratorModel,
        options: { index: 'index1', inconsistentRead: false }
      })
    }).toThrow(/Invalid option value for index and inconsistent read/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Scan({
        method: 'scan',
        options: {
          shardIndex: 123, shardCount: 1
        }
      })
    }).toThrow(/ShardIndex must be positive and smaller than shardCount/)
    expect(() => {
      // eslint-disable-next-line no-new
      new Scan({
        options: {
          shardIndex: -1, shardCount: 1
        }
      })
    }).toThrow(/ShardIndex must be positive and smaller than shardCount/)
  }

  testDefaultFlags () {
    const scan = new Scan({
      ModelCls: TestIteratorModel
    })
    expect(scan.inconsistentRead).toBe(false)
    expect(scan.shardIndex).toBe(undefined)
    expect(scan.shardCount).toBe(undefined)
    expect(scan.index).toBe(undefined)

    const query = new Query({
      ModelCls: TestIteratorModel
    })
    expect(query.allowLazyFilter).toBe(false)
    expect(query.descending).toBe(false)
    expect(query.inconsistentRead).toBe(false)
    expect(query.index).toBe(undefined)

    const scan2 = new Scan({
      ModelCls: TestIteratorModel,
      options: { index: 'index1' }
    })
    expect(scan2.index).toBe('index1')
    expect(scan2.inconsistentRead).toBe(true)

    const query2 = new Query({
      ModelCls: TestIteratorModel,
      options: { index: 'index1' }
    })
    expect(query2.index).toBe('index1')
    expect(query2.inconsistentRead).toBe(true)
  }

  testValidFlags () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: {
        allowLazyFilter: true,
        inconsistentRead: true,
        descending: true,
        index: 'index1'
      }
    })
    expect(query.descending).toBe(true)
    expect(query.allowLazyFilter).toBe(true)
    expect(query.inconsistentRead).toBe(true)
    expect(query.index).toBe('index1')

    const scan = new Scan({
      ModelCls: TestIteratorModel,
      options: {
        inconsistentRead: true,
        shardIndex: 1,
        shardCount: 2,
        index: 'index1'
      }
    })
    expect(scan.inconsistentRead).toBe(true)
    expect(scan.shardIndex).toBe(1)
    expect(scan.shardCount).toBe(2)
    expect(scan.index).toBe('index1')
  }

  testGetKeyNames () {
    const keyNames = Query.__getKeyNames({
      KEY: {
        k1: 1,
        k2: 2
      },
      SORT_KEY: {
        sk1: 3,
        sk2: 4
      }
    })
    expect(keyNames.partitionKeys).toEqual(new Set(['k1', 'k2']))
    expect(keyNames.sortKeys).toEqual(new Set(['sk1', 'sk2']))
    expect(keyNames.allKeys).toEqual(new Set(['k1', 'k2', 'sk1', 'sk2']))

    const keyNamesWithIndex = Query.__getKeyNames(TestIteratorModel, 'index2')
    expect(keyNamesWithIndex.partitionKeys).toEqual(new Set(['id1', 'sk1']))
    expect(keyNamesWithIndex.sortKeys).toEqual(new Set(['field1', 'field2']))
    expect(keyNamesWithIndex.allKeys).toEqual(new Set(['id1', 'sk1', 'field1', 'field2']))
  }

  testKeyFilters () {
    const query = new Query({
      ModelCls: TestIteratorModel
    })
    expect(() => {
      query.id1('!=', '123')
    }).toThrow('Only equality filters are allowed on partition keys')

    const query1 = new Query({
      ModelCls: TestIteratorModel
    })
    // example filter chaining start
    query1.id1('xyz').id2(123).sk1('>', '1')
    // example filter chaining end
    query1.sk2('<', '1')
    expect(() => {
      query1.__checkKeyFilters()
    }).toThrow('Filter operations on keys')
  }

  testKeyFiltersWithIndex () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: { index: 'index2' }
    })
    expect(() => {
      query.id1('!=', '123')
    }).toThrow('Only equality filters are allowed on partition keys')
    expect(() => {
      query.sk1('!=', '123')
    }).toThrow('Only equality filters are allowed on partition keys')

    query.id1('xyz').sk1(123).field1('<', '1')
    expect(() => {
      query.__checkKeyFilters()
    }).toThrow('Filter operations on keys')
  }

  testLazyFilter () {
    // query not allowing lazy filters throws when filter on non-key fields
    const query = new Query({
      ModelCls: TestIteratorModel
    })
    query.id1('123')
    expect(() => {
      query.field1('345')
    }).toThrow(/May not filter on non-key fields/)

    // query allowing lazy filters works when filter exists on non-key fields
    const query1 = new Query({
      ModelCls: TestIteratorModel,
      options: {
        allowLazyFilter: true
      }
    })
    query1.id1('123')
    expect(() => {
      query1.field1('345')
    }).not.toThrow()
  }

  testLazyFilterWithIndex () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: { index: 'index2' }
    })
    // id2 isn't a key in the index2 secondary index
    expect(() => {
      query.id2('345')
    }).toThrow(/May not filter on non-key fields/)

    // query allowing lazy filters works when filter exists on non-key fields
    const query1 = new Query({
      ModelCls: TestIteratorModel,
      options: {
        allowLazyFilter: true,
        index: 'index2'
      }
    })
    expect(() => {
      query1.field1('345')
    }).not.toThrow()
  }

  testKeyCondition () {
    const query = new Query({
      ModelCls: TestIteratorModel
    })
    expect(() => {
      query.__getKeyConditionExpression(TestIteratorModel)
    }).toThrow(/Query must contain partition key filters/)

    // example equality filter start
    query.id1('xyz')
    query.id2(321)
    // example equality filter end
    expect(query.__getKeyConditionExpression(TestIteratorModel)).toEqual([
      ['#_id=:_id'],
      { '#_id': '_id' },
      { ':_id': 'xyz' + '\0' + '321' }
    ])

    query.sk1('>', '23')
    expect(() => {
      query.__getKeyConditionExpression(TestIteratorModel)
    }).toThrow(/sk2 must be provided/)
    query.sk2('>', '34')
    expect(query.__getKeyConditionExpression(TestIteratorModel)).toEqual([
      ['#_id=:_id', '#_sk>:_sk'],
      { '#_id': '_id', '#_sk': '_sk' },
      { ':_id': 'xyz' + '\0' + '321', ':_sk': '23' + '\0' + '34' }
    ])
  }

  testKeyConditionWithIndex () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: { index: 'index1' }
    })
    expect(() => {
      query.__getKeyConditionExpression(TestIteratorModel)
    }).toThrow(/Query must contain partition key filters/)

    query.id1('xyz')
    query.id2(321)
    query.field1('>', '10')
    // Index re-uses _id/_sk if the underlying fields are the same
    expect(query.__getKeyConditionExpression(TestIteratorModel)).toEqual([
      ['#_id=:_id', '#_sk>:_sk'],
      { '#_id': '_id', '#_sk': 'field1' },
      { ':_id': 'xyz' + '\0' + '321', ':_sk': '10' }
    ])

    const query2 = new Query({
      ModelCls: TestIteratorModel,
      options: { index: 'index2' }
    })
    query2.id1('xyz').sk1('abc').field1('>', '10')
    expect(() => {
      query2.__getKeyConditionExpression(TestIteratorModel)
    }).toThrow('field2 must be provided')
    query2.field2('>', '34')
    expect(query2.__getKeyConditionExpression(TestIteratorModel)).toEqual([
      ['#_id=:_id', '#_sk>:_sk'],
      { '#_id': '_c_id1_sk1', '#_sk': '_c_field1_field2' },
      { ':_id': 'xyz' + '\0' + 'abc', ':_sk': '10' + '\0' + '34' }
    ])

    const query3 = new Query({
      ModelCls: TestIteratorModel,
      options: { index: 'index3' }
    })
    query3.field3([{ a: 10 }])
    expect(query3.__getKeyConditionExpression(TestIteratorModel)).toEqual([
      ['#_id=:_id'],
      { '#_id': '_c_field3' },
      { ':_id': '[{"a":10}]' }
    ])
  }

  testFilterCondition () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: {
        allowLazyFilter: true
      }
    })
    query.id1('xyz')
    query.id2(321)
    expect(query.__getFilterExpression()).toEqual([[], {}, {}])

    query.sk2('>', '23')
    expect(() => {
      query.__getFilterExpression()
    }).toThrow(/Filter operations on keys/)
    query.sk1('>', '123')
    expect(query.__getFilterExpression()).toEqual([[], {}, {}])

    query.field1('>', '23')
    const awsName = query.__data.field1.__awsName
    expect(query.__getFilterExpression()).toEqual([
      [`#_${awsName}>:_${awsName}`],
      { [`#_${awsName}`]: 'field1' },
      { [`:_${awsName}`]: '23' }
    ])
  }

  testFilterConditionWithIndex () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: {
        index: 'index2',
        allowLazyFilter: true
      }
    })
    query.id1('xyz')
    query.sk1('abc')
    expect(query.__getFilterExpression()).toEqual([[], {}, {}])

    query.field1('>', '23')
    expect(() => {
      query.__getFilterExpression()
    }).toThrow(/Filter operations on keys/)
    query.field2('>', '230')

    query.sk2('>', '123')
    const awsName = query.__data.sk2.__awsName
    expect(query.__getFilterExpression()).toEqual([
      [`#_${awsName}>:_${awsName}`],
      { [`#_${awsName}`]: 'sk2' },
      { [`:_${awsName}`]: '123' }
    ])
  }

  testAddConditionExpression () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: {
        allowLazyFilter: true
      }
    })
    query.field1('321')
    query.field2('321')
    const params = {}
    expect(() => {
      query.__addConditionExpression(params, '__getKeyConditionExpression')
    }).toThrow(/Query must contain partition key filters/)
    query.id1('321')
    query.id2(345)
    query.__addConditionExpression(params, '__getKeyConditionExpression')
    expect(params.KeyConditionExpression).toBe('(#_id=:_id)')

    const name1 = query.__data.field1.__awsName
    const name2 = query.__data.field2.__awsName
    query.__addConditionExpression(params, '__getFilterExpression')
    expect(params.FilterExpression).toBe(
      `(#_${name1}=:_${name1}) AND (#_${name2}=:_${name2})`
    )
  }

  testSetupParams () {
    // Changes made to returned params don't affect next call's result
    const query = new Query({
      ModelCls: TestIteratorModel
    })
    query.id1('123').id2(234)
    const expected = query.__setupParams()
    expected.change = true
    expect(query.__setupParams().change).toBe(undefined)

    // Additional changes to filter throws
    expect(() => {
      query.id1('123')
    }).toThrow(/Filter can no longer be changed/)
  }

  testSetupParamsFlag () {
    const query = new Query({
      ModelCls: TestIteratorModel,
      options: {
        descending: true
      }
    })
    query.id1('xyz')
    query.id2(123)
    const queryParams = query.__setupParams()
    expect(queryParams.ScanIndexForward).toBe(false)
    expect(queryParams.IndexName).toBe(undefined)

    const scan = new Scan({
      ModelCls: TestIteratorModel,
      options: {
        index: 'index1',
        shardIndex: 0,
        shardCount: 2
      }
    })
    const params = scan.__setupParams()
    expect(params.IndexName).toBe('index1')
    expect(params.Segment).toBe(0)
    expect(params.TotalSegments).toBe(2)
  }
}

class ScanModel extends db.Model {
  static KEY = {
    id: S.str
  }

  static FIELDS = {
    ts: S.int,
    rank: S.int.optional()
  }

  static INDEXES = {
    index1: { KEY: ['rank'], SORT_KEY: ['ts'] },
    index2: { KEY: ['id'] }
  }
}

class OrderModel extends db.Model {
  static KEY = {
    id: S.str
  }

  static SORT_KEY = {
    ts: S.int
  }
}

class ScanTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await ScanModel.createResource()

    const ts = Math.floor(new Date().getTime() / 1000) - 99999
    await db.Transaction.run(tx => {
      const models = []
      for (let index = 0; index < 4; index++) {
        models.push(ScanModel.data({
          id: this.getName(index),
          rank: index % 2,
          ts: ts
        }))
      }
      models.push(ScanModel.data({ id: this.getName(5), ts: (ts - 10) }))
      return tx.get(models, { createIfMissing: true })
    })
  }

  getName (idx) {
    return `scantest-${idx}`
  }

  async testScanFetchFew () {
    const validate = async (expected, opt = {}) => {
      const [models, nextToken] = await db.Transaction.run(async tx => {
        // example scanHandle start
        const scan = tx.scan(ScanModel, opt)
        // example scanHandle end
        return scan.fetch(3)
      })
      expect(models.length).toBe(expected)
      expect(nextToken).toBeDefined()
    }
    await validate(3)
    await validate(3, { index: 'index1' })
    await validate(3, { index: 'index2' })
  }

  async testScanFetchAll () {
    const validate = async (expected, opt = {}) => {
    // Fetching all rows
      const [models, nextToken] = await db.Transaction.run(async tx => {
        const scan = tx.scan(ScanModel, opt)
        return scan.fetch(100)
      })
      expect(models.length).toBe(expected)
      expect(nextToken).toBeUndefined()
    }
    await validate(5)
    await validate(4, { index: 'index1' })
    await validate(5, { index: 'index2' })
  }

  async testScanFetchNext () {
    const validate = async (expected, opt = {}) => {
      const [models, nextToken] = await db.Transaction.run(async tx => {
        const ret = []
        // example scan start
        const scan = tx.scan(ScanModel, opt)
        const [page1, nextToken1] = await scan.fetch(2)
        const [page2, nextToken2] = await scan.fetch(10, nextToken1)
        // example scan end
        ret.push(...page1, ...page2)
        return [ret, nextToken2]
      })
      expect(models.length).toBe(expected)
      expect(nextToken).toBeUndefined()
    }
    await validate(5)
    await validate(4, { index: 'index1' })
    await validate(5, { index: 'index2' })
  }

  async testScanRunFew () {
    const validate = async (expected, opt = {}) => {
      // Run a few rows
      const ret = await db.Transaction.run(async tx => {
        const scan = tx.scan(ScanModel, opt)
        const models = []
        for await (const model of scan.run(3)) {
          models.push(model)
        }
        return models
      })
      expect(ret.length).toBe(expected)
    }

    await validate(3)
    await validate(3, { index: 'index1' })
  }

  async testScanRunAll () {
    const validate = async (expected, opt = {}) => {
      // Run all row
      const ret = await db.Transaction.run(async tx => {
        const scan = tx.scan(ScanModel, opt)
        const models = []
        for await (const model of scan.run(100)) {
          models.push(model)
        }
        return models
      })
      expect(ret.length).toBe(expected)
    }
    await validate(5)
    await validate(4, { index: 'index1' })
  }

  // It's easier to test is here then in the TTL suite
  async testTTL () {
    // Turn on TTL locally
    ScanModel.EXPIRE_EPOCH_FIELD = 'ts'

    const validate = async (opt = {}) => {
      const models = await db.Transaction.run(async tx => {
        const scan = tx.scan(ScanModel, opt)
        const ms = []
        for await (const m of scan.run(100)) {
          ms.push(m)
        }
        return ms
      })
      expect(models.length).toBe(0)
    }

    await validate()
    await validate({ index: 'index1' })
    await validate({ index: 'index2' })
    ScanModel.EXPIRE_EPOCH_FIELD = undefined
  }

  async testWrite () {
    const models = await db.Transaction.run(async tx => {
      const scan = tx.scan(ScanModel)
      const ms = []
      for await (const m of scan.run(1)) {
        ms.push(m)
      }
      expect(tx.__writeBatcher.__allModels.length).toBe(1)

      ms[0].ts--
      return ms
    })
    expect(models.length).toBe(1)

    await db.Transaction.run(async tx => {
      const scan = tx.scan(ScanModel, { index: 'index1' })
      const models2 = (await scan.fetch(10))[0]
      expect(models2.length).toBe(4)
      expect(tx.__writeBatcher.__allModels.length).toBe(0)
    })
  }

  async testInconsistentRead () {
    const scanRet = await db.Transaction.run(async tx => {
      const scan = tx.scan(ScanModel, { inconsistentRead: true })
      return scan.__setupParams().ConsistentRead
    })
    expect(scanRet).toBe(false)
  }

  async testSharding () {
    await db.Transaction.run(async tx => {
      const scan = tx.scan(ScanModel, { shardCount: 2, shardIndex: 0 })
      return scan.fetch(10)
    })
    await db.Transaction.run(async tx => {
      const scan2 = tx.scan(ScanModel, { index: 'index1', shardCount: 2, shardIndex: 0 })
      return scan2.fetch(10)
    })
  }
}

class QueryModel extends db.Model {
  static KEY = {
    id1: S.str,
    id2: S.int
  }

  static SORT_KEY = {
    sk1: S.str
  }

  static FIELDS = {
    field: S.int
  }

  static INDEXES = {
    index1: { KEY: ['id1', 'id2'], SORT_KEY: ['field'] },
    index2: { KEY: ['id1', 'sk1'], SORT_KEY: ['id2'] },
    index3: { KEY: ['id1'], SORT_KEY: ['field'] }
  }
}

class SortModel extends db.Model {
  static KEY = {
    id: S.str
  }

  static SORT_KEY = {
    sk: S.obj({
      arr: S.arr(S.int)
    })
  }

  static INDEXES = {
    index: { KEY: ['sk'] }
  }
}

class QueryTest extends BaseTest {
  async beforeAll () {
    await super.beforeAll()
    await QueryModel.createResource()
    await SortModel.createResource()
    await OrderModel.createResource()

    await db.Transaction.run(tx => {
      const models = [
        QueryModel.data({
          id1: '1',
          id2: 1,
          sk1: '0',
          field: 0
        }),
        QueryModel.data({
          id1: '1',
          id2: 1,
          sk1: '123',
          field: 1
        }),
        SortModel.data({
          id: '0',
          sk: { arr: [1] }
        }),
        SortModel.data({
          id: '0',
          sk: { arr: [2] }
        })
      ]
      return tx.get(models, { createIfMissing: true })
    })
  }

  async testQueryId () {
    const results = await db.Transaction.run(async tx => {
      // example queryHandle start
      const query = tx.query(QueryModel)
      // example queryHandle end
      query.id1('1')
      query.id2(1)
      return (await query.fetch(10))[0]
    })
    expect(results.length).toBe(2)
    expect(results[0].sk1).toBe('0')
    expect(results[1].sk1).toBe('123')
  }

  async testQueryIdWithIndex () {
    await db.Transaction.run(async tx => {
      let query = tx.query(QueryModel, { index: 'index1' })
      query.id1('1').id2(1)
      let result = (await query.fetch(10))[0]
      expect(result.length).toBe(2)
      expect([result[0].field, result[1].field]).toEqual([0, 1])

      query = tx.query(QueryModel, { index: 'index2' })
      query.id1('1').sk1('0')
      result = (await query.fetch(10))[0]
      expect(result.length).toBe(1)
      expect(result[0].sk1).toBe('0')
    })
  }

  async testQueryNonExistentId () {
    const results = await db.Transaction.run(async tx => {
      const query = tx.query(QueryModel)
      query.id1('invalid')
      query.id2(1)
      return (await query.fetch(10))[0]
    })
    expect(results.length).toBe(0)

    const results2 = await db.Transaction.run(async tx => {
      const query = tx.query(QueryModel, { index: 'index1' })
      query.id1('invalid')
      query.id2(1)
      return (await query.fetch(10))[0]
    })
    expect(results2.length).toBe(0)
  }

  async testQuerySortKey () {
    const results = await db.Transaction.run(async tx => {
      const query = tx.query(QueryModel)
      query.id1('1')
      query.id2(1)
      query.sk1('prefix', '1')
      return (await query.fetch(10))[0]
    })
    expect(results.length).toBe(1)
  }

  async testQuerySortKeyWithIndex () {
    const results2 = await db.Transaction.run(async tx => {
      const query = tx.query(QueryModel, { index: 'index1' })
      query.id1('1')
      query.id2(1)
      query.field('>', 0)

      return (await query.fetch(10))[0]
    })
    expect(results2.length).toBe(1)
  }

  async testBetweenSortKey () {
    const results = await db.Transaction.run(async tx => {
      const query = tx.query(SortModel)
      query.id('0')

      query.sk('between', { arr: [1] }, { arr: [1] })
      return (await query.fetch(10))[0]
    })
    expect(results.length).toBe(1)
  }

  async testQueryDescending () {
    const results = await db.Transaction.run(async tx => {
      // example descending start
      const query = tx.query(QueryModel, { descending: true })
      // example descending end
      query.id1('1')
      query.id2(1)
      // example queryFetch start
      const [results1, nextToken1] = await query.fetch(1)
      const [results2, nextToken2] = await query.fetch(999, nextToken1)
      expect(nextToken2).toBeUndefined()
      // example queryFetch end
      return [...results1, ...results2]
    })
    expect(results.length).toBe(2)
    expect(results[0].sk1).toBe('123')
    expect(results[1].sk1).toBe('0')
  }

  async testQueryDescendingWithIndex () {
    await db.Transaction.run(async tx => {
      const query = tx.query(QueryModel, { index: 'index3', descending: true })
      query.id1('1')
      const results = (await query.fetch(10))[0]
      expect(results.length).toBe(2)
      expect(results[0].field).toBe(1)
    })
  }

  async testLazyFilter () {
    const results = await db.Transaction.run(async tx => {
      // example lazyFilter start
      const query = tx.query(QueryModel, { allowLazyFilter: true })
      // example lazyFilter end
      query.id1('1')
      query.id2(1)
      query.field(0)
      const ret = []
      for await (const data of query.run(10)) {
        ret.push(data)
      }
      return ret
    })
    expect(results.length).toBe(1)
  }

  async testLazyFilterWithIndex () {
    await db.Transaction.run(async tx => {
      // example lazyFilter start
      const query = tx.query(QueryModel, { index: 'index2', allowLazyFilter: true })
      // example lazyFilter end
      query.id1('1').sk1('0')
      const results = (await query.fetch(10))[0]
      expect(results.length).toBe(1)
      expect(results[0].field).toBe(0)

      const query2 = tx.query(QueryModel, { index: 'index2', allowLazyFilter: true })
      // example lazyFilter end
      query2.id1('1').sk1('0')
      query2.field('>', '0')
      const results2 = (await query2.fetch(10))[0]
      expect(results2.length).toBe(0)
    })
  }

  async testInconsistentRead () {
    const queryRet = await db.Transaction.run(async tx => {
      // example inconsistentQuery start
      const query = tx.query(QueryModel, { inconsistentRead: true })
      query.id1('123').id2(123)
      // example inconsistentQuery end
      return query.__setupParams().ConsistentRead
    })
    expect(queryRet).toBe(false)
  }

  async testOrdering () {
    // scan should return rows in numeric order, not text order, e.g.
    // 1, 10, 2... is wrong
    const id = uuidv4()
    await db.Transaction.run(async tx => {
      for (let index = 0; index < 11; index++) {
        tx.create(OrderModel, { id, ts: index })
      }
    })

    await db.Transaction.run(async tx => {
      const query = tx.query(OrderModel).id(id)
      let expectedTS = 0
      for await (const model of query.run(999)) {
        expect(model.ts).toBe(expectedTS)
        expectedTS++
      }
    })
  }
}

runTests(
  IteratorTest,
  QueryTest,
  ScanTest
)
