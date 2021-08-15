const AWSError = require('./aws-error')
const { ITEM_SOURCE } = require('./utils')

/**
 * DataBase iterator. Supports query and scan operations.
 * @private
 */
class __DBIterator {
  static OPERATION_NAME = undefined

  constructor ({
    Cls,
    writeBatcher,
    options
  }) {
    const {
      inconsistentRead = false
    } = options || {}

    this.__writeBatcher = writeBatcher
    this.__ModelCls = Cls
    this.__fetchParams = undefined
    this.inconsistentRead = inconsistentRead
  }

  __setupParams () {
    if (!this.__fetchParams) {
      const params = {
        TableName: this.__ModelCls.fullTableName,
        ConsistentRead: !this.inconsistentRead
      }
      this.__fetchParams = params
    }
    return this.__fetchParams
  }

  /**
   * Get one batch of items, by going through at most n items. Return a
   * nextToken for pagination.
   *
   * @param {Integer} n The max number of items to check (not return). When
   *   filtering is done, items not passing the filter conditions will not be
   *   returned, but they count towards the max.
   * @param {Object} [nextToken=undefined] A token for fetching the next batch.
   *   It is returned from a previous call to __getBatch. When nextToken is
   *   undefined, the function will go from the start of the DB table.
   *
   * @return {Tuple(Array<Model>, String)} A tuple of (items, nextToken). When nextToken
   *   is undefined, the end of the DB table has been reached.
   */
  async __getBatch (n, nextToken = undefined) {
    this.__setupParams()

    const params = this.__fetchParams
    params.Limit = n
    if (!nextToken) {
      delete params.ExclusiveStartKey
    } else {
      params.ExclusiveStartKey = nextToken
    }
    const op = this.constructor.OPERATION_NAME
    const result = await this.documentClient[op](this.__fetchParams).promise()
      .catch(
        // istanbul ignore next
        e => { throw new AWSError(op, e) }
      )

    const models = result.Items.map(item => {
      const m = new this.__ModelCls(ITEM_SOURCE.SCAN, false, item)
      if (m.__hasExpired) {
        return undefined
      }
      this.__writeBatcher.track(m)
      return m
    }).filter(m => !!m)

    return [
      models,
      result.LastEvaluatedKey
    ]
  }

  /**
   * Fetch n items from DB, return the fetched items and a token to next page.
   *
   * @param {Integer} n The number of items to return.
   * @param {Object} [nextToken=undefined] A token for fetching the next batch.
   *   It is returned from a previous call to fetch. When nextToken is
   *   undefined, the function will go from the start of the DB table.
   *
   * @return {Tuple(Array<Model>, String)} A tuple of (items, nextToken). When nextToken
   *   is undefined, the end of the DB table has been reached.
   */
  async fetch (n, nextToken = undefined) {
    const ret = []
    while (ret.length < n) {
      const [ms, nt] = await this.__getBatch(
        n - ret.length,
        nextToken
      )
      ret.push(...ms)
      nextToken = nt // Update even if nt is undefined, to terminate pagination
      if (!nt) {
        // no more items
        break
      }
    }
    return [ret, nextToken]
  }

  /**
   * A generator API for retrieving items from DB.
   *
   * @param {Integer} n The number of items to return.
   */
  async * run (n) {
    let fetchedCount = 0
    let nextToken
    while (fetchedCount < n) {
      const [models, nt] = await this.__getBatch(
        Math.min(n - fetchedCount, 50),
        nextToken
      )
      for (const model of models) {
        yield model
      }
      if (!nt) {
        return
      }
      fetchedCount += models.length
      nextToken = nt
    }
  }
}

/**
 * Scan handle for constructing filter expressions
 */
class Scan extends __DBIterator {
  static OPERATION_NAME = 'scan'
}

module.exports = {
  Scan
}
