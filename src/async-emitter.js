const uuidv4 = require('uuid').v4

class AsyncEmitter {
  constructor () {
    this.handlers = {}
  }

  __addHandler (event, handler, handlerName = uuidv4()) {
    const handlers = this.handlers[event] || {}
    this.handlers[event] = handlers
    if (handlers[handlerName]) {
      throw new Error('Handler with the same name already exists')
    }
    handlers[handlerName] = handler
    return handlerName
  }

  removeHandler (event, handlerName) {
    const handlers = this.handlers[event] || {}
    delete handlers[handlerName]
  }

  once (event, handler, handlerName = undefined) {
    return this.__addHandler(event, {
      handler,
      once: true
    }, handlerName)
  }

  on (event, handler, handlerName = undefined) {
    return this.__addHandler(event, {
      handler,
      once: false
    }, handlerName)
  }

  async emit (event, ...args) {
    const entries = Object.entries(this.handlers[event] || {})
    for (const [handlerName, handlerData] of entries) {
      const { handler, once } = handlerData
      if (once) {
        this.removeHandler(event, handlerName)
      }
      await handler(...args)
    }
  }
}

module.exports = AsyncEmitter
