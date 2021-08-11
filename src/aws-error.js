// istanbul ignore file
class AWSError extends Error {
  constructor (action, e) {
    super(`Failed to ${action} with error ${e}`)
    this.code = e.code
  }
}

module.exports = AWSError
