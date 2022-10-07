const { Key } = require('./key')

/**
 * Data includes a model's key and non-key fields.
 * @param {Object} [fields] field (non-key) values
 */
class Data extends Key {
  constructor (Cls, encodedKeys, keyComponents, fields) {
    super(Cls, encodedKeys, keyComponents)
    this.data = fields
  }

  get key () {
    return new Key(this.Cls, this.encodedKeys, this.keyComponents)
  }

  get vals () {
    return { ...this.keyComponents, ...this.data }
  }
}

module.exports = {
  Data
}
