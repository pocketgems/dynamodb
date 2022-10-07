const { BaseTest, runTests } = require('@pocketgems/unit-test')

const Emitter = require('../src/async-emitter')

class EmitterTest extends BaseTest {
  async testHandleOnce () {
    const mock = jest.fn()
    const emitter = new Emitter()
    emitter.once('abc', mock)
    await emitter.emit('aaa')
    expect(mock).toHaveBeenCalledTimes(0)

    await emitter.emit('abc')
    expect(mock).toHaveBeenCalledTimes(1)

    await emitter.emit('abc')
    expect(mock).toHaveBeenCalledTimes(1)
  }

  async testRemovingHandler () {
    const emitter = new Emitter()
    emitter.removeHandler('abc', 'aaa')

    const name = emitter.once('abc', () => {})
    emitter.removeHandler('abc', name)
    expect(emitter.handlers.abc).not.toHaveProperty(name)
  }

  async testRepeatedHandler () {
    const mock = jest.fn()
    const emitter = new Emitter()
    emitter.on('abc', mock)

    await emitter.emit('aaa')
    expect(mock).toHaveBeenCalledTimes(0)

    await emitter.emit('abc')
    expect(mock).toHaveBeenCalledTimes(1)

    await emitter.emit('abc')
    expect(mock).toHaveBeenCalledTimes(2)
  }

  async testNamedHandler () {
    const mock = jest.fn()
    const emitter = new Emitter()
    const name = emitter.once('abc', mock, 'h1')
    expect(name).toBe('h1')

    expect(() => {
      emitter.once('abc', mock, 'h1')
    }).toThrow('Handler with the same name h1 already exists')

    emitter.removeHandler('abc', 'h1')
    await emitter.emit('abc')
    expect(mock).toHaveBeenCalledTimes(0)

    const name2 = emitter.on('abc', mock, 'h2')
    expect(name2).toBe('h2')
    expect(() => {
      emitter.once('abc', mock, 'h2')
    }).toThrow('Handler with the same name h2 already exists')
  }

  testDefaultHandlerName () {
    const mock = jest.fn()
    const emitter = new Emitter()
    const name = emitter.once('abc', mock)
    expect(name).toBeDefined()

    const name2 = emitter.on('abc', mock)
    expect(name2).toBeDefined()
  }

  async testArgPassing () {
    const mock = jest.fn()
    const emitter = new Emitter()
    emitter.once('a', mock)
    await emitter.emit('a', 1, 2, 3)
    expect(mock).toHaveBeenLastCalledWith(1, 2, 3)

    emitter.once('b', mock)
    await emitter.emit('b', 3, 2, 1)
    expect(mock).toHaveBeenLastCalledWith(3, 2, 1)
  }
}

runTests(EmitterTest)
