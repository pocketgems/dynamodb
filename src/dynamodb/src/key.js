/**
 * Key uniquely identifies a model.
 */
class Key {
  /**
   * @param {Model} Cls a Model class
   * @param {Object} encodedKeys map of encoded partition and sort key
   * @param {Object} keyComponents key component values
   * @private
   */
  constructor (Cls, encodedKeys, keyComponents) {
    this.Cls = Cls
    this.encodedKeys = encodedKeys
    this.keyComponents = keyComponents
  }
}

class UniqueKeyList extends Array {
  constructor (...keys) {
    super(...keys)
    const hashes = keys.map(key => this.constructor.getKeyHash(key))
    this.__keyHashes = new Set(hashes)
  }

  static getKeyHash (key) {
    const { _id, _sk } = key.encodedKeys
    return `${key.Cls.name}::${_id}::${_sk}`
  }

  push (...keys) {
    for (const key of keys) {
      const keyHash = this.constructor.getKeyHash(key)
      if (!this.__keyHashes.has(keyHash)) {
        this.__keyHashes.add(keyHash)
        super.push(key)
      }
    }
  }

  filter (...args) {
    return Array.prototype.filter.bind(this, ...args)
  }
}

module.exports = {
  Key,
  UniqueKeyList
}
