# Data Modeling library <!-- omit in toc -->
This library is used to interact with the DynamoDB NoSQL database. It provides high-level abstractions to structure data and prevent race conditions.

- [Core Concepts](#core-concepts)
  - [Minimal Example](#minimal-example)
  - [Tables](#tables)
    - [Keys](#keys)
    - [Fields](#fields)
    - [Schema Enforcement](#schema-enforcement)
    - [Custom Methods](#custom-methods)
  - [Transactions](#transactions)
    - [ACID Properties](#acid-properties)
    - [Automatic Optimistic Locking (AOL)](#automatic-optimistic-locking-aol)
    - [Retries](#retries)
    - [Read-Only](#read-only)
    - [Warning: Race Conditions](#warning-race-conditions)
    - [Warning: Side Effects](#warning-side-effects)
    - [Per-request transaction](#per-request-transaction)
  - [Operations](#operations)
    - [Addressing Items](#addressing-items)
    - [Create](#create)
    - [Read](#read)
      - [Create if Missing](#create-if-missing)
      - [Read Consistency](#read-consistency)
      - [Batch Read](#batch-read)
    - [Write](#write)
  - [Performance](#performance)
    - [DAX](#dax)
    - [Blind Writes](#blind-writes)
    - [incrementBy()](#incrementby)
- [Niche Concepts](#niche-concepts)
  - [Key Encoding](#key-encoding)
  - [Nested Transactions are NOT Nested](#nested-transactions-are-not-nested)
  - [Unsupported DynamoDB Features](#unsupported-dynamodb-features)
  - [Table Creation & Persistence](#table-creation--persistence)
  - [Sort Keys](#sort-keys)
  - [Overlapping Models](#overlapping-models)
- [Library Collaborator's Guide](#library-collaborators-guide)
  - [Conventions](#conventions)
  - [AOL](#aol)
  - [Transactions](#transactions-1)
- [Appendix](#appendix)


# Core Concepts
Data is organized into tables. Each row in a table is called an _Item_, which
is composed of one or more _Fields_. Each item uniquely identified by a
[_Key_](#keys) (more on this later).

## Minimal Example
Define a new table like this:
```javascript <!-- embed:../test/unit-test-dynamodb.js:scope:Order -->
class Order extends db.Model {
  static FIELDS = {
    product: S.str,
    quantity: S.int
  }
}
```

Then we can create a new item (effectively a row in the database):
```javascript
const id = uuidv4()
tx.create(Order, { id, product: 'coffee', quantity: 1 })
```

Later, we can retrieve it from the database and modify it:
```javascript <!-- embed:../test/unit-test-dynamodb.js:scope:DBReadmeTest:testMinimalExample:Example -->
    // Example
    await db.Transaction.run(async tx => {
      const order = await tx.get(Order, id)
      expect(order.id).toBe(id)
      expect(order.product).toBe('coffee')
      expect(order.quantity).toBe(1)
      order.quantity = 2
    })
```


## Tables

### Keys
Each item is uniquely identified by a key. By default, the key is composed of a
single field named `id` which has the format of a UUIDv4 string (e.g.,
`"c40ef065-4034-4be8-8a1d-0959695b213e"`) typically produced by calling
`uuidv4()`, as shown in the minimal example above. An item's key cannot be
changed.

You can override the default and define your key to be composed of one _or
more_ fields with arbitrary
[Todea schema](./schema.md)s (`R`):
```javascript <!-- embed:../test/unit-test-dynamodb.js:scope:RaceResult -->
class RaceResult extends db.Model {
  static KEY = {
    raceID: S.int,
    runnerName: S.str
  }
}
```

Access each component of a key just like any other field:
```javascript <!-- embed:../test/unit-test-dynamodb.js:scope:DBReadmeTest:testKeys -->
  async testKeys () {
    await RaceResult.createUnittestResource()
    await db.Transaction.run(async tx => {
      const raceResult = await tx.get(
        RaceResult,
        { raceID: 99, runnerName: 'Bo' },
        { createIfMissing: true })
      expect(raceResult.raceID).toBe(99)
      expect(raceResult.runnerName).toBe('Bo')
    })
  }
```

It is best practice for keys to have semantic meaning whenever possible. In
this example, each runner finishes each race just one time so making the key a
combination of those values is ideal. This is better than a meaningless random
value because this:
  1. Enforces the constraint that each runner finishes each race no more than
     once. If the ID was a random value, we could accidentally create two race
     results for one runner in the same race.
  1. Enables us efficiently construct the ID from relevant information (e.g.,
     to check if a runner finished a specific race). If the ID was was a random value, we'd have to do some sort of search to figure out the ID associated
     with a given race ID and runner name (slow because this would involve a
     database query instead of a simple local computation!).

Note: Keys are table-specific. Two different items in different tables may have
the same key.


### Fields
Fields are pieces of data attached to an item. They are defined similar to
`KEY`:
```javascript <!-- embed:../test/unit-test-dynamodb.js:scope:ModelWithFields -->
class ModelWithFields extends db.Model {
  static FIELDS = {
    someInt: S.int.min(0),
    someBool: S.bool,
    someObj: S.obj().prop('arr', S.arr(S.str))
  }
}
```

Fields can be configured to be optional, immutable and/or have default values:
 * `optional()` - unless a field is marked as optional, a value must be
   provided (i.e., it cannot be omitted or set to `undefined`)
 * `readOnly()` - if a field is marked as read only, it cannot be changed once
   the item has been created
 * `default()` - the default value for a field
    * This value gets deep copied so you can safely use non-primitive type like
      an object as a default value.
    * The default value is assigned to a field when:
       * An item is created and no value is specified for the value.
       * An item is fetched and is is missing the specified field _AND_ the
         field is required.
    * The default value is _not_ assigned to an optional field that is missing
      when it is fetched from the database.
```javascript <!-- embed:../test/unit-test-dynamodb.js:scope:ModelWithComplexFields -->
class ModelWithComplexFields extends db.Model {
  static FIELDS = {
    aNonNegInt: S.int.min(0),
    anOptBool: S.bool.optional(), // default value is undefined
    // this field defaults to 5; once it is set, it cannot be changed (though
    // it won't always be 5 since it can be created with a non-default value)
    immutableInt: S.int.readOnly().default(5)
  }
}
```
```javascript <!-- embed:../test/unit-test-dynamodb.js:section:example1122start:example1122end -->
      // can omit the optional field
      const item = tx.create(ModelWithComplexFields, {
        id: uuidv4(),
        aNonNegInt: 0,
        immutableInt: 3
      })
      expect(item.aNonNegInt).toBe(0)
      // omitted optional field => undefined
      expect(item.anOptBool).toBe(undefined)
      expect(item.immutableInt).toBe(3)

      // can override the default value
      const item2 = tx.create(ModelWithComplexFields, {
        id: uuidv4(),
        aNonNegInt: 1,
        anOptBool: true
      })
      expect(item2.aNonNegInt).toBe(1)
      expect(item2.anOptBool).toBe(true)
      expect(item2.immutableInt).toBe(5) // the default value
      // can't change read only fields:
      expect(() => { item2.immutableInt = 3 }).toThrow(
        'immutableInt is immutable so value cannot be changed')
```


### Schema Enforcement
A model's schema (i.e., the structure of its data) is enforced by this library
— _NOT_ the underlying database! DynamoDB, like most NoSQL databases, is
effectively schemaless (except for the key). This means each item may
theoretically contain completely different data. This normally won't be the
case because `db.Model` enforces a consistent schema on items in a table.

However, it's important to understand that this schema is _only_ enforced by
`db.Model` and not the underlying database. This means **changing the model
does not change any underlying data** in the database. For example, if we make
a previously optional field required, old items which omitted the value will
still be missing the value.

The schema is checked as follows:
  1. When a field's value is changed, it is validated. If a value is a
     reference (e.g., an object or array), then changing a value inside the reference does _not_ trigger a validation check.
```javascript
         // fields are checked immediately when creating a new item; this throws
         // S.ValidationError because someInt should be an integer
         const data = {
           id: uuidv4(),
           someInt: '1', // does not match the schema S.int)!
           someBool: true,
           someObj: { arr: [] }
         }
         tx.create(ModelWithFields, data) // throws because someInt is invalid

         data.someInt = 1
         const x = tx.create(ModelWithFields, data)

         // fields are checked when set too
         x.someBool = 1 // throws because the type should be boolean not int
         x.someObj = {} // throws because the required "arr" key is missing
         x.someObj = { arr: [5] } // throws b/c this arr must contain strings
         x.someObj = { arr: ['ok'] } // ok!

         // changes within a non-primitive type aren't detected or validated
         // until we try to write the change so this next line won't throw!
         x.someObj.arr.push(5)
```

  2. Any fields that will be written to the database are validated prior to
     writing them. This occurs when a [transaction](#transactions) commit
     starts. This catches schema validation errors like the one on the last
     line of the previous example.

  3. Keys are validated whenever they are created or read, like these examples:
```javascript
         const compoundID = { raceID: 1, runnerName: 'Alice' }
         // each of these three trigger a validation check (to verify that
         // compoundID contains every key component and that each of them meet
         // their respective schemas requirements)
         RaceResult.key(compoundID)
         tx.create(RaceResult, compoundID)
         await tx.get(RaceResult, compoundID)
```

  4. Fields validation can be manually triggered:
```javascript
         x.getField('someObj').validate()
```

### Custom Methods
As you've noticed, key components and fields are simply accessed by their names
(e.g., `raceResult.runnerName` or `order.product`). You can also define
instance methods on your models to provide additional functionality:
```javascript <!-- embed:../test/unit-test-dynamodb.js:scope:OrderWithPrice -->
class OrderWithPrice extends db.Model {
  static FIELDS = {
    quantity: S.int,
    unitPrice: S.int.desc('price per unit in cents')
  }

  totalPrice (salesTax = 0.1) {
    const subTotal = this.quantity * this.unitPrice
    return subTotal * (1 + salesTax)
  }
}
```
And use them like you'd expect:
```javascript
const order = tx.create(OrderWithPrice, { id, quantity: 2 , unitPrice: 200 })
expect(order.totalPrice(0.1)).toBeCloseTo(440)
```


## Transactions
A transaction is a function which contains logic and database operations. A
transaction guarantees that all _database_ side effects (e.g., updating an
item) execute in an all-or-nothing manner, providing both
[ACID](#acid-properties) properties as well as
[Automatic Optimistic Locking](#automatic-optimistic-locking-aol).


### ACID Properties
[ACID](https://en.wikipedia.org/wiki/ACID) properties are commonly provided by
traditional, transaction-processing databases:

 * _Atomicity_ - every database operation (e.g., an update) will succeed, or
   none will succeed. The database will never be partially updated.
 * _Consistency_ - data written to the database will always be consistent with
   the constraints specified by the models (e.g., it is not possible to store a
   string in an integer field).
 * _Isolation_ - each transaction will appear to operate sequentially;
   uncommitted data cannot be read.
 * _Durability_ - if a transaction succeeds, any data that is changed will be
   remembered. There is no chance of it being lost (e.g., due to a power
   outage).


### Automatic Optimistic Locking (AOL)
AOL ensures that a transaction succeeds only if accessed fields have not
changed. This relieves developer's from the time-consuming and error-prone
process of manually writing the necessary
[conditions checks](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)
to prevent many insidious and hard-to-debug race condition bugs.

_Contention_ occurs when a transaction's inputs change before the transaction
is completed. When this happens, a transaction will fail (if our inputs
changed, then AOL assumes any updates we requested may no longer be valid). If
a transaction fails, the default behavior is to automatically retry it up to
three times. If all the retries fail due to contention, the transaction will
throw a `db.TransactionFailedError` exception. If there is some permanent error (e.g,
you tried to create a new item but it already exists) then the transaction will
throw a `db.ModelAlreadyExistsError` exception without additional retries.

Consider this guestbook model:
```javascript
class Guestbook extends db.Model {
  static FIELDS = { names: S.arr(S.str) }
}
```

If two (or more!) transactions try to update the guestbook at the same time,
it's vital that each successfully adds a name to the guestbook exactly once.
Without transactions, something like this could happen:

1. Alice and Bob both get the current names in the guestbook (an empty array).
1. Alice adds her name to the empty array, updating the DB to `names=["Alice"]`
1. Bob adds his name to the empty array, updating the DB to `names=["Bob"]`
1. The database is left with just one name in the guestbook. Uh-oh!

AOL places _conditions_ on the update to ensure race conditions like this
cannot occur. AOL effectively changes the above requests to:

2. _If_ the guestbook is empty, then update it to `names=["Alice"]`.
2. _If_ the guestbook is empty, then update it to `names=["Bob"]`..

Exactly one of these two will succeed, causing the guestbook to no longer be
empty. The other transaction to fail because the condition ("If the guestbook
is empty") will not be true. It will retry, fetching the guestbook again and
creating a new request for the database:

3. If the guestbook is `names=["Alice"]`, then update it to
   `names=["Alice", "Bob"]`.


### Retries
When a transaction fails due to contention, it will retry after a short, random delay. Randomness helps prevent conflicting transactions from conflicting again when they retry. Transaction retry behaviors can be customized:
```javascript
const retryOptions = {
  retries: 4, // 1 initial run + up to 4 retry attempts = max 5 total attempts
  initialBackoff: 100, // 100 milliseconds (+/- a small random offset)
  maxBackoff: 500 // no more than 500 milliseconds
}
await db.Transaction.run(retryOptions, async tx => {
  // you can also manually force your transaction to retry by throwing a
  // custom exception with the "retryable" property set to true
  const error = new Error()
  error.retryable = true
  throw error
})
// Exponential backoff function doubles the backoff each time (up to the max)
// t=0ms, initial run
// t=100ms, retry 1 (backed off for 100ms)
// t=300ms, retry 2 (backed off again, this time for 200ms)
// t=700ms, retry 3 (backed off again, this time for 400ms)
// t=1200ms, retry 4 (backed off for 500ms this time; was capped by maxBackoff)
// fail
```


### Read-Only
You can ensure a transaction does not make any database changes by setting the
`readOnly` option to true, or calling `tx.makeReadOnly()`:
```javascript
const readOnlyOption = { readOnly: true }
await db.Transaction.run(readOnlyOption, async tx => { /* ... */ })
await db.Transaction.run(async tx => {
  tx.makeReadOnly()
  // ...
})
```


### Warning: Race Conditions
Race conditions are still possible! Consider a ski resort which records some
stats about skiers and lifts:
```javascript
class SkierStats extends db.Model {
  static KEY = { resort: S.str }
  static FIELDS = { numSkiers: S.int.min(0).default(0) }
}
class LiftStats extends db.Model {
  static KEY = { resort: S.str }
  static FIELDS = { numLiftRides: S.int.min(0).default(0) }
}
```

We can correctly update these numbers in a transaction like this:
```javascript
async function liftRideTaken(resort, isNewSkier) {
  await db.Transaction.run(async tx => {
    const opts = { createIfMissing: true }
    const [skierStats, liftStats] = await Promise.all([
      !isNewSkier ? Promise.resolve() : tx.get(SkierStats, resort, opts),
      tx.get(LiftStats, resort, opts)])
    if (isNewSkier) {
      skierStats.numSkiers += 1
    }
    liftStats.numLiftRides += 1
  })
}
```

However, if we try to read them we can't guarantee a consistent snapshot:
```javascript
const skierStats = await tx.get(SkierStats, resort)
const liftStats = await tx.get(LiftStats, resort)
```

This sequence is possible:
  1. We issue requests to read SkierStats and LiftStats, as above.
  1. We call `liftRideTaken('someResort', true)`
  1. The request to read skier stats complete: `numSkiers=0`
  1. The `liftRideTaken('someResort', true)` completes, transactionally
     updating the database to `numSkiers=1` and `numLiftRides=1`.
  1. The request to read lift stats complete: `numLiftRides=1` _!!!_
  1. Our application code thinks there was one lift ride taken, but no skiers.

To ensure this does not occur, use `db.get()` to fetch both items in a single request:
```javascript
const [skierStats, liftStats] = await tx.get([
  SkierStats.key(resort),
  LiftStats.key(resort)
])
// Caution: Don't pass inconsistentRead=true if you need a consistent snapshot!
```

Under the hood, when multiple items are fetched with strong consistency, DynamoDB's `transactGetItems` API is called to prevent races mentioned above.


### Warning: Side Effects
Keep in mind that transactions only guarantee all-or-nothing (or more
precisely, exactly once or not at all semantics) for _database_ operations. If
the application code which defines the transaction has side effects, those side
effects may occur even if the transaction doesn't commit. They could even occur
multiple times (if your transaction retries).
```javascript
  await db.Transaction.run(async tx => {
    const item = await tx.get(...)
    item.someInt += 1
    if (item.someInt > 10) {
      // making an HTTP request is a side effect!
      await got('https://example.com/theItemHassomeIntBiggerThan10')
    }
  })
```

In this example, the HTTP request might be completed one or more times, even if
the transaction never completes successfully!


### Per-request transaction
Each request handled by our [API Definition library](api.md) is wrapped in a transaction. Read more about it [here](api.md#database-transactions).


## Operations
_All_ databases operations occur in the scope of a transaction. We typically
name the transaction object `tx` in code. This section discusses the operations
supported by `tx`.


### Addressing Items
Database operations always occur on a particular item. The canonical way to identify a particular item is:
```javascript
MyModel.key({ /* a map of key component names to their values */ })
Order.key({ id: uuidv4() })
RaceResult.key({ raceID: 1, runnerName: 'Dave' })
```

For models which have only a single key field, you _may_ omit the field name:
```javascript
Order.key(uuid4())
```

The `db.Key` object produced by this `key()` method is used as the first
argument to database operations:
```javascript
tx.get(Order.key(id))
```

For convenience, you may also split the model class and key values up into two
arguments:
```javascript
tx.get(Order, id)
tx.get(RaceResult, { raceID, runnerName })
```


### Create
`tx.create()` instantiates a new item in local memory. This method is a local,
**synchronous** method (no network traffic is generated). If an item with the
same key already exists, a `db.ModelAlreadyExistsError` is thrown when the
transaction attempts to commit (without retries, as we don't expect items to be
deleted).

To create an item, you need to supply the model (the type of item you're
creating) and a map of its initial values:
```javascript
tx.create(Order, { id, product: 'coffee', quantity: 1 })
```


### Read
`tx.get()` **asynchronously** retrieves data from the database. Network traffic
is generated to ask the database for the data as soon as the method is call,
but other work can be done while waiting.
```javascript
const orderPromise = tx.get(Order, id)
// do some other work
const order = await orderPromise // block until the data has been retrieved
```

`tx.get()` accepts an additional options to configure its behavior:
  * `createIfMissing` - see [Create if Missing](#create-if-missing)
  * `inconsistentRead` - see [Read Consistency](#read-consistency)


#### Create if Missing
If the item does not exist in the database, then by default the returned value
will be `undefined`. You may ask for it to instead be created if it does not
exist. To do this, you need to supply not only the item's key, but also the
data you want it to have _if_ it does not yet exist:
```javascript
const dataIfOrderIsNew = { id, product: 'coffee', quantity: 1 }
const order = await tx.get(Order, dataIfOrderIsNew, { createIfMissing: true })
if (order.isNew) { // you can check if the item already existed or not
  // ...
}
```

The `isNew` property is set when the model is instantiated (after receiving the
database's response to our data request). When the transaction commits, it will
ensure that the item is still being created if `isNew=true` (i.e., the item
wasn't created by someone else in the meantime) or still exists if
`isNew=false` (i.e., the item hasn't been deleted in the meantime).


#### Read Consistency
Inconsistent reads provide eventual consistency. This allows reading data from
our [DAX](#dax) cache or any database node (even if they _may_ be out of sync).
This differs from consistent reads (the default) which provide strong
consistency but are less efficient (and twice as costly) as inconsistent reads.
```javascript
await tx.get(Order, id, { inconsistentRead: true })
```

#### Batch Read
It is also possible to call `tx.get()` with an array of keys in order to fetch
many things at once:
```javascript
const [order1, order2, raceResult] = await tx.get([
  Order.key(id),
  Order.key(anotherID),
  RaceResult.key({ raceID, runnerName })
])
```

This can also be combined with `createIfMissing`:
```javascript
const [order1, order2, raceResult] = await tx.get([
  Order.data({ id, product: 'coffee', quantity: 1 }),
  Order.data({ id: anotherID, product: 'spoon', quantity: 10 }),
  RaceResult.data({ raceID, runnerName })
], { createIfMissing: true })
```

* When (`inconsistentRead=`**`false`**), the items are fetched transactionally
  in a single network request that guarantees we receive a consistent snapshot
  (see [race conditions](#warning-race-conditions) for more about this).
* When `inconsistentRead=`**`true`** the items are fetched (usually) with one
  network request. This is faster than making many separate `tx.get()`
  requests, especially when data can be read from [DAX](#DAX). This operation
  is faster than a consistent batch read, but it does not guarantee a
  consistent snapshot and only provides eventual consistency.

Note: Any given item can only be fetched once during a transaction. It is an
error to try to fetch the same data twice.


### Write
To modify data in the database, simply modify fields on an item created by
`tx.create()` or fetched by `tx.get()`. When the transaction commits, all
changes will be written to the database automatically.


## Performance

### DAX
With [DAX](https://aws.amazon.com/dynamodb/dax/) enabled (the default),
inconsistent reads can resolve within 10ms as opposed to the consistent
counterpart which will likely take 40-50ms.


### Blind Writes
Blind updates write an item to the DB without reading it first. This is useful
when we already know model's fields' values and wish to update them without the overhead of an unnecessary read:
```javascript
// this updates the specified order item to quantity=2, but only if the current
// quantity === 1 and product === 'coffee'
tx.update(Order, { id, quantity: 1, product: 'coffee' }, { quantity: 2 })
```

To maintain consistency, old values _must_ be provided for each field to be
updated. In addition, any values used to derive the new value should be
included in the old values. Failure to do so may result in race condition bugs.

Similarly, items can be blindly created or overwritten with `createOrPut`
method. This is useful when we don't care about the previous value (if any).
For example, maybe we're tracking whether a customer has used a particular feature or not. When they use it, we may just want to blindly record it:
```javascript
class LastUsedFeature extends db.Model {
  static KEY = {
    user: S.str,
    feature: S.str
  }
  static FIELDS = { epoch: S.int }
}
// ...
tx.createOrPut(HasTriedFeature,
  // these are the values we expect (must include all key components);
  // this call fails if the data exists AND it doesn't match these values
  { user: 'Bob', feature: 'refer a friend' },
  // this contains the new value(s); if a value is undefined then the
  // field will be deleted (it must be optional for this to be allowed)
  { epoch: 123 })
```

Both of these methods are synchronous, local methods like `tx.create()`. They
return immediately and do not perform any network traffic. All network traffic
related to these are generated as part of any writes processed when the
transaction commits.


### incrementBy()
To achieve higher write throughput and reduce contention, you can use
`incrementBy()` to mutate numeric fields. This can be used when you want to
increment (or decrement) a number's value but don't care about its old value:
```javascript
class WebsiteHitCounter extends db.Model {
  static FIELDS = { count: S.int.min(0) }
}

async function slowlyIncrement(id) {
  const counter = await tx.get(WebsiteHitCounter, id)
  // here we read and write the data, so the library will generate an
  // update like "if count was N then set count to N + 1"
  counter.count += 1
  expect(counter.getField('count').canUpdateWithoutCondition).toBe(true)
}

async function quicklyIncrement(id) {
  const counter = await tx.get(WebsiteHitCounter, id)
  // since we only increment the number and never read it, the library will
  // generate an update like "increment quantity by 1" which will succeed no
  // matter what the original value was
  counter.getField('count').incrementBy(1)
  expect(counter.getField('count').canUpdateWithoutCondition).toBe(false)
}
```

Using the `incrementBy()` only helps if you're not going to read the field
being incremented (though it never hurts to use it):
```javascript
async function bothAreJustAsFast(id) {
  const counter = await tx.get(WebsiteHitCounter, id)
  if (counter.count < 100) { // stop counting after reaching 100
    // this is preferred here b/c it is simpler and just as fast in this case
    // counter.count += 1

    // isn't any faster because we have to generate the condition
    // expression due to the above if condition which read the count var
    counter.getField('count').incrementBy(1)

    expect(counter.getField('count').canUpdateWithoutCondition).toBe(false)
  }
}
```

Using `incrementBy()` on a field whose value is `undefined` is invalid and will
throw an exception.


# Niche Concepts

## Key Encoding
Under the hood, a database key can only be a single attribute. We always store
that attribute as a string. We compute this string's value by first sorting the
names of the components of the key. Then we compute the string representation
of each component's value (with `JSON.stringify()`, except for string values
which don't need to be encoded like that). Finally, we concatenate these values
(in order of their keys) and separate them with null characters. An encoded key
would look like this:
```javascript
const item = tx.create(RaceResult, { raceID: 123, runnerName: 'Joe' })
expect(item._id).toBe('123\0Joe')

// the encoded key is also contained in the output of Model.key():
const key = RaceResult.key({ runnerName: 'Mel', raceID: 123 })
expect(key.Cls).toBe(RaceResult)
expect(key.encodedKeys._id).toBe('123\0Mel')

// the encoded sort key, if any, will be stored in the _sk attribute
```

For this reason, string values cannot contain the null character. If you need
to store a string with this value, your best option is to probably nest it
inside of an object:
```javascript
class StringKeyWithNullBytes extends db.Model {
  static KEY = { id: S.obj().prop('raw', S.str) }
}
tx.create(StringKeyWithNullBytes, {
  id: {
    raw: 'I can contain \0, no pr\0bl\0em!'
  }
})
```


## Nested Transactions are NOT Nested
Nested transactions like this should be avoided:
```javascript
await Transaction.run(async outerTx => {
  // ...
  await Transaction.run(async innerTx => {
    // ...
  }
}
```
The inner transaction, if it commits, will commit first. If the outer
transaction is retried, the inner transaction _will be run additional times_.


## Unsupported DynamoDB Features
This library does not yet support:
   - Query
   - Scan
   - Indexing


## Table Creation & Persistence
When the localhost server runs, it generates `config/init-resources.yml` based
on the models you've defined (make sure to export them from your service!).
On localhost, the data persists until you shut down the service. If you add new
models or change a model (particularly its key structure), you will need to restart your service to incorporate the changes.

Along the same lines, keep in mind that the localhost database is _not_ cleared
in between test runs. Any data added to the localhost database will remain
until the service is restarted. This can help you debug issues, but it also
means you should not create items with a fixed ID as part of a unit test (use
`uuidv4()` to get a random ID value so it won't clash with a future run of the
unit tests.)

Whenever a service is deployed to test or prod, any table which did not
previously exist is created. _If a table is removed, its data will still be
retained._ It must be manually deleted if its data is no longer needed. This
is a safety precaution to avoid data loss.

Be careful about changing your models: remember that changing the model does
_not_ change anything in the database. Be especially wary about changing the
key structure — it will probably cause serious problems.


## Sort Keys
The key which uniquely identifies an item in a table has two components:
  1. `KEY` - this defines the table's _partition_ key. A table's items are
     typically stored across many different database nodes. The _hash_ of the
     partition key is used to determine which node hosts which items, though
     you don't normally need to be aware of this detail.
  1. `SORT_KEY` - this defines the table's _optional_ _sort_ key. Sort keys are
     also part of an item's unique identity, but doesn't affect partitioning.
     Items with the same partition key but different sort keys will all be
     stored on the same node.

Accessing many small items from the same table with the same partition key but
different sort keys is just as efficient as lumping them all into one large
item. Performance will be better when you only need to access a subset of these smaller items.

This is better than using different partition keys (or different tables) for
the smaller items because then doing transactions involving multiple items
would probably incur a performance penalty as the transaction would need to run
across multiple nodes instead of just one.

When using sort keys, be careful not to overload an single database node. For example, it'd be awful to have a model like this:
```javascript
class CustomerData extends db.Model {
  static KEY = { store: S.str }
  static SORT_KEY = { customer: S.str }
}
tx.create(CustomerData, { store: 'Walmart', customer: uuidv4() })
```

In this case, every customer for a store would be on the same database node.
It'd be much better for the customer to be part of the partition key instead of
the sort key. Sort keys should be used for highly related data that will often
be used together. It should not be used for overly large or unrelated data.


## Overlapping Models
Two models may be stored in the same physical table (after all, the underlying tables don't enforce a schema; each item could theoretically be different, except for the key structure).

This may be desirable on rare occasions when two related types of data should
be co-located on the same database node (by putting them in the same table and
giving them the same partition key value, but differing [sort key](#sort-keys)
values).

This example has just one table, Inventory, which is populated by two different (but related) models:
```javascript
class Inventory extends db.Model {
  // we override the default table name so that our subclasses all use the same
  // table name
  static tableName = 'Inventory'
  static KEY = { userID: S.str }

  static get SORT_KEY () {
    return { typeKey: S.str.default(this.INVENTORY_ITEM_TYPE) }
  }

  static get FIELDS () {
    return { stuff: S.obj().default({}) }
  }

  static INVENTORY_ITEM_TYPE () { throw new Error('To be overwritten') }
}
class Currency extends Inventory {
  static INVENTORY_ITEM_TYPE = 'money'
}
class Weapon extends Inventory {
  static INVENTORY_ITEM_TYPE = 'weapon'
  static FIELDS = {
    ...super.FIELDS,
    weaponSkillLevel: S.int
  }
}

// both items will be stored in the Inventory; both will also be stored on
// the same database node since they share the same partition key (userID)
tx.create(Currency, {
  userID,
  typeKey: Currency.INVENTORY_ITEM_TYPE,
  stuff: { usd: 123, rmb: 456 }
})
tx.create(Weapon, {
  userID,
  typeKey: Weapon.INVENTORY_ITEM_TYPE,
  stuff: { ax: {/* ... */} },
  weaponSkillLevel: 13
})
```


# Library Collaborator's Guide

## Conventions
* **Convention** as follows:
  * Prefix `__` indicate private usages.
  * Classes prefixed with `__` are private to package.
     * Variables prefixed with `__`, are private to class.

  * Classes without `__` prefix are public.
     * Variables prefixed with `__`, are at least private to package.

* **Code** for this library is all in one file, located under `services/sharedlib/src/dynamodb.js`.

* **Unit Tests** are available under `services/sharedlib/test/unit-test-dynamodb*.js`.


## AOL
This library automates optimistic locking by tracking fields accessed and constructing expressions under the hood, thus entirely avoid hand crafting requests like above. Rules are as following:

* For **ConditionExpression**
    - If a model does not exists on server:
        - Set expression to `'attribute_not_exists(id)'`
    - Else
        - For each field read or written:
            - Append `field=oldValue`

* For **UpdateExpression**
    - For each field written:
        - If `newValue === undefined`:
            - Append `'field'` to _REMOVE_ section
        - If `newValue !== undefined`:
            - Append `'field=newValue'` to _SET_ section


## Transactions
Our `Transaction` class, combines AOL and DynamoDB's transactWrite with the following strategy:

* Individual get operations are allowed within a transaction context.
* Models read are tracked by the transaction context.
* Models mutated are written to DB using one single transactWrite operation on commit.
* TransactWrite request is constructed using the following rules:
    * For each readonly items:
        * Append ConditionExpressions generated using AOL
    * For each read-write items:
        * Append UpdateExpression generated using AOL.
        * Append ConditionExpressions generated using AOL
* Transaction commits when the transaction context / scope is exited.
* If a `retryable` error or `ConditionalCheckFailedException` or `TransactionCanceledException` is thrown during transactWrite operation, transaction will be retried.
* If all retries failed, a `TransactionFailedError` will be thrown.

When more than one item is accessed and/or updated, this library issues a
`transactWriteItems` call to DynamoDB. For performance reasons, if exactly one
items was accessed and updated, this library uses a non-transactional
`writeItem` call to provide the same ACID properties a transactWrite could
provide.


# Appendix
The samples in this readme can be found in the APIs defined for unit testing
this library in `services/sharedlib/test/unit-test-dynamodb.js` in the
`DBReadmeTest` class.
