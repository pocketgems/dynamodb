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

module.exports = {
  Key
}
