# Data Modeling library for DynamoDB

This library wraps the dynamodb js SDK by AWS and supports:

- Structured and validated models.
- Automatic optimistic locking.
- Built-in retries & exponential backoffs.
- DAX

It does not yet support:

- Query
- Scan
- Indexing

# Contents
* [Getting Started](#getting-started)
* [Core Concepts](#core-concepts)
    * [Automatic Optimistic Locking](#automatic-optimistic-locking-aol)
    * [Transactions](#transactions)
    * [Composite IDs](#composite-ids)
* [Examples](#examples)
* [Advanced Examples](#advanced-examples)

## Getting Started
* **Convention** as follows:

  * Prefix `__` indicate private usages.
  * Classes prefixed with `__` are private to package.
     * Variables prefixed with `__`, are private to class.

  * Classes without `__` prefix are public.
     * Variables prefixed with `__`, are at least private to package.

* **Code** for this libaray is all in one file, located under `services/sharedlib/src/dynamodb.js`.

* **Unit Tests** are availble under `services/sharedlib/test/unit-test-dynamodb*.js`.

## Core Concepts
### Automatic Optimistic Locking (AOL)
Simutaneous access to a single DB item happens very often. It creates opportunities for race conditions where updates made by one request is overwritten by another request. In a tranditional RDBM, this is easily avoided using transactions. However, due to the lack of native transaction support by DynamoDB, an alternative strategy needs to be employeed.

This library implements an Automatic Optimistic Locking (AOL) strategy using DynamoDB's ConditionalExpression with `update` request to avoid such race conditions at an item level. For example, consider the following requests:

```javascript
async function request1 () {
    // Get player
    const levelsUp = player.guild ? 2 : 1
    player.level += levelsUp
    // Write player
}

async function request2 () {
    // Get player
    console.assert(player.level > 10, 'Player level too low to join a guild')
    player.guild = 'newName'
    // write player
}
```

In a naive implementation using DynamoDB's `put` method, if `request1` and `request2` get invoked simultaneously on a player with data ```{ id: '1', level: 11}```, there are at least 2 possible outcomes:

* **Outcome1**

    1. request1 and request2 gets player's data
    1. request1 puts { id: '1', level: 12 }
    1. request2 puts { id: '1', level: 11, guild: 'my guild' }

* **Outcome2**

    1. request1 and request2 gets player's data
    1. request2 puts { id: '1', level: 11, guild: 'my guild' }
    1. request1 puts { id: '1', level: 12 }

Depending on the ordering of the 2 requests, one of them will appear to be never called.

Optimistic locking solves this problem well by causing the slower request to throw `ConditionalCheckFailedException`. A native DynamoDB implementation would look like this:

```javascript
// For request1
await dynamoDBDocumentClient.update({
  TableName: 'Player',
  Key: { id: '1' },
  UpdateExpression: 'SET level=:newLevel',
  // Realizing guild was read as a condition for level, and guild didn't exist
  // AND level was read to calculate the final result for level
  ConditionExpression: 'attribute_not_exists(guild) AND level=:oldLevel',
  ExpressionAttributeValues: {
    ':newLevel': 12,
    ':oldLevel': 11,
  }
}).promise()

// For request2
await dynamoDBDocumentClient.update({
  TableName: 'Player',
  Key: { id: '1' },
  UpdateExpression: 'SET guild=:newName',
  // Realizing level was read as a condition for guild, and level was 11
  // AND guild didn't exist.
  ConditionExpression: 'level=:oldLevel AND attribute_not_exists(guild)',
  ExpressionAttributeValues: {
    ':newName': 'my guild',
    ':oldLevel': 11,
  }
}).promise()
```

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

### Transactions
While AOL solved races between updates involving a single item, updates involving multiple items from multiple tables are still subject to similar races.

In addition, although DynamoDB does support transactGet and transactWrite operations, they only guarantee atomicity and isolation properties, and do not improve on consistency nor durability properties. To learn more about dynamodb's ACID properties please read [AWS DynamoDB's official doc](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html). **TL;DR** DynamoDB makes sure transactGet and any other write operations are mutually exclusive, similarly transactWrite and any other operations (get or write) are mutually exclusive. On conflict, `TransactionCanceledException` will be thrown for the transact operations. However, the native dynamodb client does not prevent overriding data in back-to-back transactWrites.

For example, 2 requests read data back to back, then commits changes back to back. The transactGet and transactWrite calls happens sequentially (e.g. get1->get2->write1->write2), thus no `TransactionCanceledException` is thrown, and both transactWrites completes successfully. But the data will appear as if only the slower request happened.

This library addresses this problem using the provided Transaction class, which combines AOL and DynamoDB's transactWrite with the following strategy:

* Individual get operations are allowed within a transaction context.
* Models read are tracked by the transaction context.
* Models mutated are written to DB using one single transactWrite operation on commit.
* TransactWrite request is constructed using the following rules:
    * For each readonly items:
        * Append ConditionExpressions generated using AOL
    * For each readwrite items:
        * Append UpdateExpression generated using AOL.
        * Append ConditionExpressions generated using AOL
* Transaction commits when the transaction context / scope is exited.
* If a `retryable` error or `ConditionalCheckFailedException` or `TransactionCanceledException` is thrown during transactWrite operation, transaction will be retried.
* If all retries failed, a `TransactionFailedError` will be thrown.

### Composite IDs
DynamoDB supports using a combinations of HASH key and RANGE key to identify items. This library refers to IDs like this as CompositeIDs.

- HASH key (id) is used with a hash function to partition items into different hosting machines in the DB backend. By doing so, the DB guarantees items with the same HASH key are always assigned to the same DB node, and items are evenly distributed across all DB nodes, thus evely distributes work loads across machines.
- RANGE key is optionally used as a supplement to uniquely identify an item, but doesn't affect partitioning at all.

For example, consider a table for storing player inventory items. In this example, player1 and player2 happen to be assigned to one DB node / machine, and others to another.

* Inventory: partition 1

    | id (HASH) | typeKey (RANGE) | items           | weaponLevel |
    | --------  | --------------- | --------------- | ----------- |
    | player1   | money           | { diamonds: 1 } |             |
    | player1   | weapon          | { uzi: 1 }      | { uzi: 1 }  |
    | player2   | weapon          | { ak47: 1 }     |             |

* Inventory: partition 2

    | id (HASH) | typeKey (RANGE) | items           | weaponLevel  |
    | --------  | --------------- | --------------- | ------------ |
    | player4   | money           | { coins: 100 }  |              |
    | player4   | weapon          | { colt: 1 }     |              |
    | player5   | weapon          | { apache: 1 }   | { apache: 1} |

**Code in Action**
Jump to [example code below](#composite-ids-1).

**Considerations**

* Using CompositeIDs might make sense because:
    1. Individual items are smaller, potentially avoid reaching DB read / write size limits and reduces contention.
    1. Items with the same HASH key (id) are stored on the same machine (if items with the same HASH key can fit onto one machine), making accessing multiple items with the same HASH key as perfomant as having one large item.

* However, it might be undesirable when
    1. A partition key is accessed far more often than others, thus putting much more traffic on one node.
    1. Some partition key stores much more data than others.

An example where CompositeIDs are undesireable would be supporting a store selling champion skins. One might design the table like the following, so listing skins for each champion in store would be quick:

| id (HASH) | skin (RANGE)  |
| --------- | ------------- |
| jinx      | star guardian |
| jinx      | project       |
| vayne     | soulstealer   |
| vayne     | firecracher   |
| kog'maw   | pug'maw       |
| kog'maw   | battlecast    |

However, if a limited quantity sales event for a specific champion takes place, traffic will be heavily concentrated to one DB node and create a performance bottle neck.


## Examples
Additional examples are availble in each individual services. You can find the most extensive example in sharedlib "service".

### Import
```javascript
const db = require('dynamodb')()
```

### Start A Transaction
Starting a transaction to begin interacting with DynamoDB. By design, every operation must go through transactions to guarantee correctness.

```javascript
await db.Transaction.run(async tx => {
  ...
  return 123 // Optionally return some value
})
```

### Define A Model
```javascript
class Player extends db.Model {
  constructor () {
    super()
    this.guildName = db.StringField()
    this.smr = db.ObjectField({ default: {} })
    this.level = db.IntegerField({ default: 0 })
  }
}
```

### Fields
There are 5 basic Field types: `NumberField`, `StringField`, `BooleanField`, `ArrayField` and `ObjectField`.

When creating fields some options can be given. For example:

```javascript
db.ObjectField({ optional: true, default: {} })
db.NumberField({ immutable: false })
db.StringField({ keyType: 'RANGE' })
```

For more detailed documentation on these options, please read **API Documnetation**.

### Create An Item
`create()` instantiates a new item in local memory; it is entirely a local, synchronous method (no network traffic is generated). AOL makes sure when the request to write the item is sent to DB, if an item with the same key already exists, a TransactionFailedError is thrown.

```javascript
tx.create(Player, { id: 'id', guild: 'guild' })
await tx.get(Player, 'id', { createIfMissing: true })
```

### Get An Item
```javascript
await tx.get(Player, 'id')
await tx.get(Player, { id: 'id', rangeKey: 123 })
await tx.get(Player.key('id'))
await tx.get([
  db.Key(Player, 'id'),
  db.Key(Guild, 'gid')
])
```

### Check Item Existence
```javascript
let p = tx.create(Player, { id: 'id', rangeKey: 123, guild: 'guild' })
console.assert(p.isNew, "Must be new")

p = await tx.get(Player, 'A new ID')
console.assert(!p, "Don't expect any returned value")

p = await tx.get(Player, 'A new ID', { createIfMissing: true })
console.assert(p.isNew, "isNew indicates whether it exists on server")
```
In above cases, isNew is expectation established when the model is instantiated either using local data or snapshots of server. Calling isNew doesn't not poll server again. On write, if the model was created by someone else, a TransactionFailedError will be thrown.

### Updating An Item
```javascript
let p = tx.create(Player, { id: 'id', rangeKey: 123, guild: 'guild' })

// Notice guildName was set to StringField in constructor.
// But syntax for reading / writing this property is the same
// as a normal Object property.
p.guildName = 'myGuildName'
p.level += 1
```

This library also supports "blindly" writing an item to the DB without reading first. It is useful when we already know model's fields' values and wish to update them.

```javascript
tx.update(Player, { id: 'id', guild: 'guild' }, { guild: 'newGuild' })
```

To maintain consistency, when updating a model, old values must be provided for each field to be updated. In addition, any values used to derive the new value should be included in the old values.

Likewise, items can be "blindly" created or overwritten with `createOrPut` method.

```javascript
tx.createOrPut(Player,
  {
    id: 'id',
    guild: 'oldGuild' // Condition for Put. Ignored for creating.
  },
  {
    guild: 'newGuild',
    fieldToRemove: undefined // To be removed.
  })
```

### Write An Item
It would be confusing if this doc didn't have a section on `Write An Item`, but really there is no need for it.

Any item created or updated are written to DB when Transaction scope is exited.

When more than one item is accessed and updated, this library issues a transactWriteItems call to DynamoDB. For performance reasons, if exactly one items was accessed and updated, this library uses a non-transactional writeItem call to provide the same ACID properties a transactWrite could provide.

### Transaction Retries
Transaction supports customizations on retry and backoff behaviors.

```javascript
// See API Documentation for default retries.
// It's 3 now, but may be altered in code.
await db.Transaction.run({
    // 5 retries
    //  = 1 initial run + 5 retry attempts
    //  = 6 attempts before failing.
    retries: 5
}, async tx => {
    ...
})
```

There is some randomness built into backoffs to avoid having 2 conflicting transactions to conflict again on the next retry, when their retry & backoff schedule overlaps. Consult API Documentation for details.

```javascript
await db.Transaction.run(
{
  retries: 4,
  initialBackoff: 100,
  maxBackoff: 500
}, async tx => {
  const error = new Error()
  error.retryable = true // To throw an retryable error.
  throw error
})
// Suppose exponential backoff function = (currentDt) => 2 * currentDt
// t=0ms, initial run
// t=100ms, retry 1 (dt=100)
// t=300ms, retry 2 (dt=200)
// t=700ms, retry 3 (dt=400)
// t=1200ms, retry 4 (dt is capped at 500ms)
// fail.
```


## Advanced Examples

### Field Schema
In addition to supporting model level schema via typed Field properties, this library also supports Field level schema via the schema option. The option value may be a ***fluent-schema*** object or a raw json schema object. Read docs for fluent-schema and json schema for a complete list of json structures the validation logic supports.

```javascript
const S = require('fluent-schema')

class SchemaModel extends db.Model {
  constructor () {
    super()
    this.str = db.StringField({ schema: S.string().maxLength(2) })
    this.obj = db.ObjectField({
      schema: S.object()
        .prop('str', S.string())
        .prop('arr', S.array().items(S.string()))
    })
  }
}
```

Importantly, validity of fields are enforced in 2 cases:

  1. When `model.field = ...` is called.
  2. When model is written.

```javascript
const fut = db.Transaction.run(async tx => {
  const model = await tx.get(SchemaModel, 'mymodel')
  // model.str = '123' // would have thrown 'string too long' on this line

  // nested json mutations are not checked synchronously,
  // validation happens on model write instead.
  model.obj.str = []
})
await fut  // invalid model.obj is caught now.
```

This behavior means that non-backward compatible changes (adding schema or changing existing schema) are not caught until the field is changed or the model is written.

### Updating Item Without AOL
For a higher write throughput and less contention, avoiding AOL might be desirable sometimes. This can be done with access directly to model's fields:

```javascript
const p = await tx.get(Player, 'id')

// p.level += 1
// results in
// UpdateExpression: 'level=2'
// ConditionExpression: 'level=1'

if (p.level > 10) {
  p.getField('level').incrementBy(1)
  // results in
  // UpdateExpression: 'level=level+1'
  // No ConditionExpression
  // Notice the read `p.level>10` didn't trigger AOL
  // since the subsequent update happened through `incrementBy()`.
}
```

### Inconsistent Read
This library supports inconsistent reads for improved performance.

- Consistent reads will always access DB with strong consistency which involves waiting for all DB replication nodes to reach synchronization.
- Inconsistent reads will allow eventual consistency, which will allow reading data from memcache or any DB nodes before nodes are in-sync.

With DAX enabled, inconsistent reads can resolve within 10ms as opposed to the consistent conterpart which will likely take 40~50ms. AOL will prevent errorneous updates to the DB.

```javascript
await tx.get(Player, 'A new ID', { inconsistentRead: true })
```

### Constructor Parameters
Sometimes additional parameters may be needed for Model construction. It can be done like this:

```javascript
class MyModel extends db.Model {
  constructor (myArg) {
    super()
    this.myArg = myArg
  }
}
```
Then construct a model with:

```javascript
tx.create(Player, { id: 'id', rangeKey: 123, guild: 'guild' })
await tx.get(Player, 'A new ID', { inconsistentRead: true, someOtherParams: 123 })
```

### Nested Transactions
Due to Javascript's lack of context manager support, nested transactions are not supported.

```javascript
await Transaction.run(async outerTx => {
  ...
  await Transaction.run(async innerTx => {
    ...
  }
}
```
is **almost** the same as

```javascript
await Transaction.run(async innerTx => {
  // Notice inner transaction is committed first.
  ...
}
await Transaction.run(async outerTx => {
  ...
}
```

The core difference is that in the first example, the inner transaction may be run multiple times if the outter transaction is retried. Whereas, in the second example, the 2 transactions are completely commited separately.

### Helpers in Transactions

Due to the lack of context manager again, nested helper functions needing access to transaction object must pass the tx object down like below:

```javascript
async function boo (tx) {
  // do something
}

async function foo (tx) {
  return boo(tx)
}

await db.Transaction.run(async tx => {
  return foo(tx)
})
```

When a function may be used as a standalone tx function or a helper for another tx, one could do the following:

```javascript
async function txFoo (tx) {
  if (!tx) {
    // Start a transaction context if there isn't one already
    return await db.Transaction.run(txFoo)
  }
  // do something
}

await db.Transaction.run(async tx => {
  txFoo(tx)
  // Do something else
})

// or
await txFoo()
```

### Composite IDs

The inventory example mentioned [here](#composite-ids) can be implemented in code like this:

```javascript
class Inventory extends db.Model {
  constructor () {
    super()
    this.typeKey = db.StringField({ keyType: 'RANGE' })
    this.items = db.ObjectField({ default: {} })
  }

  static get tableName () {
    return 'Inventory'
  }

  static typeKey () {
    throw new Error('To be overritten')
  }
}

class MoneyInventory extends Inventory {
  static typeKey () {
    return 'money'
  }
}

class WeaponInventory extends Inventory {
  constructor () {
    super()
    this.weaponLevel = db.ObjectField()
  }

  static typeKey () {
    return 'weapon'
  }
}
```

Simultaneously invoking the following methods will never cause contentions:

```javascript
async function addWeapon (weapon) {
  await db.Transaction.run(async tx => {
    const weaponInventory = await tx.get(WeaponInventory, this.pgid)
    weaponInventory.items[weapon]++
  })
}

async function buyCoins (amount) {
  await db.Transaction.run(async tx => {
    const moneyInventory = await tx.get(MoneyInventory, 'p1')
    const items = moneyInventory.items
    const coinPackCost = 1
    if (items.diamonds > coinPackCost) {
      items.diamonds -= coinPackCost
      items.coins += 100
    }
  })
}
```
