// istanbul ignore file
class AWSError extends Error {
  constructor (action, e) {
    super(`Failed to ${action} with error ${e}`)
    this.code = e.name
    this.name = e.name
    this.message = e.message
    this.retryable = e.retryable
    this.allErrors = e.allErrors
  }
}

module.exports = AWSError
