const { BaseTest, runTests } = require('../base-unit-test')
const db = require('../db-with-field-maker')

const Filter = db.__private.Filter

class FilterTest extends BaseTest {
  testCreation () {
    expect(() => {
      // eslint-disable-next-line no-unused-vars
      const temp = new Filter()
    }).toThrow('Filter expects 4 constructor inputs')

    expect(() => {
      // eslint-disable-next-line no-unused-vars
      const temp = new Filter('abc', 'abc', 'abc', undefined)
    }).toThrow('Filter must be created for query or scan')

    expect(() => {
      // eslint-disable-next-line no-unused-vars
      const temp = new Filter('query', 'abc', 'abc', '123')
    }).toThrow('keyType must be one of PARTITION, SORT and undefined')

    let temp = new Filter('query', 'abc', 'abc', 'PARTITION')
    expect(temp).toBeDefined()
    temp = new Filter('scan', 'abc', 'abc', 'SORT')
    expect(temp).toBeDefined()
  }

  testQueryFilter () {
    // Calling filter populates filter condition fields correctly
    const filter = new Filter('query', 'x', '1', undefined)
    filter.filter('==', 1)
    expect(filter.__operation).toBe('==')
    expect(filter.__value).toBe(1)
    expect(filter.conditions).toEqual(['#_1=:_1'])
    expect(filter.attrNames).toEqual({ '#_1': 'x' })
    expect(filter.attrValues).toEqual({ ':_1': 1 })
  }

  testDupFilter () {
    // Attempts to filter on the same property results in an error
    const filter = new Filter('query', 'x', '1', undefined)
    filter.filter('==', 1)
    expect(() => {
      filter.filter('==', 2)
    }).toThrow(/Filter on field x already exists/)
  }

  testOperationTranslation () {
    // Conversion from natural operations to db ops
    const helper = (op, expected) => {
      const filter = new Filter('query', 'x', '1', undefined)
      filter.filter(op, 1)
      expect(filter.conditions).toEqual([`#_1${expected}:_1`])
    }
    helper('==', '=')
    helper('!=', '<>')
    helper('<', '<')
    helper('<=', '<=')

    let filter = new Filter('query', 'x', '1', undefined)
    filter.filter('between', 1, 2)
    expect(filter.conditions).toEqual(['#_1 BETWEEN :_1Lower AND :_1Upper'])

    filter = new Filter('query', 'x', '1', 'SORT')
    filter.filter('prefix', '123')
    expect(filter.conditions).toEqual(['begins_with(#_1,:_1)'])
  }

  testInvalidFilterOperation () {
    const filter = new Filter('query', 'x', '1', undefined)
    expect(() => {
      filter.filter('invalid', 123)
    }).toThrow(/Invalid filter operation/)

    expect(() => {
      filter.filter('between', 123)
    }).toThrow(/"between" operator requires 2 value inputs/)

    expect(() => {
      filter.filter('between', 2, 1)
    }).toThrow(/"between" operator must be in ascending order/)

    expect(() => {
      const scan = new Filter('scan', 'x', '1', undefined)
      scan.filter('prefix', '123')
    }).toThrow('Prefix filters are only allowed on sort keys')

    expect(() => {
      const scan = new Filter('scan', 'x', '1', 'SORT')
      scan.filter('prefix', '123')
    }).toThrow(/Invalid "prefix" operation for scan/)

    expect(() => {
      const scan = new Filter('scan', 'x', '1', 'SORT')
      scan.filter('!=', '123')
    }).toThrow('Inequality filters are not allowed on keys')
  }

  testLock () {
    const filter = new Filter('query', 'x', '1', undefined)
    filter.lock()
    expect(() => {
      filter.filter('==', 1)
    }).toThrow(/Filter can no longer be changed/)
  }
}

runTests(FilterTest)
