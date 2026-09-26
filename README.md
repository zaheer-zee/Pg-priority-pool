# pg-priority-pool

> *"Think dynamic, not linear. True scalability lies in prioritizing chaos, not just waiting in line."*
> 
> **Built by ZaheerChoudhari**

A priority queue wrapper for node-postgres (`pg`) connection pools.

By default, the `pg` connection pool operates on a First-In, First-Out (FIFO) basis when all connections are exhausted. `pg-priority-pool` intercepts this mechanism, allowing you to assign a priority to your database queries. High-priority queries skip ahead of low-priority background tasks when connections become available, preventing starvation of critical user-facing requests under heavy load.

## Installation

```bash
npm install pg-priority-pool pg
```

*(Note: `pg` is required as a peer/normal dependency).*

## Usage

The API is intentionally designed to be a drop-in replacement for `pg.Pool`. The only difference is that you can pass an optional `priority` argument to `.query()` and `.connect()`.

**Lower numbers mean higher priority. The default priority is 10.**

### Basic Queries

```javascript
const { PriorityPgPool } = require('pg-priority-pool');

const pool = new PriorityPgPool({
  user: 'dbuser',
  host: 'database.server.com',
  database: 'mydb',
  password: 'secretpassword',
  port: 5432,
  max: 10, // Max number of connections
  
  // Dynamic Priority Aging (Anti-starvation)
  // E.g., 1000 means a query's priority score decreases (improves) by 1 for every 1000ms it waits.
  agingIntervalMs: 1000 
});

// A critical user-login query (Priority 1)
pool.query('SELECT * FROM users WHERE email = $1', ['user@example.com'], 1)
  .then(res => console.log('Login success:', res.rows[0]));

// A heavy background report query (Priority 20)
// If all connections are busy, this will wait in the queue while priority 1 queries execute
pool.query('SELECT * FROM massive_log_table', [], 20)
  .then(res => console.log('Background report finished'));
```

### Checking out a client (Transactions)

You can also request a priority client checkout for transactions.

```javascript
async function runHighPriorityTransaction() {
  // Check out a client with priority 1
  const client = await pool.connect(1); 
  
  try {
    await client.query('BEGIN');
    await client.query('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    // Releasing the client automatically triggers the next query in the priority queue
    client.release();
  }
}
```

## Contributing

Open to collaborate! Send PRs, let's discuss and merge to improve our app together.

## How it works
`pg-priority-pool` maintains its own internal connection tracker (`activeCount`). 
- When `activeCount < max`, your queries pass straight through to the underlying `pg.Pool` with zero overhead.
- When `activeCount >= max`, your queries are pushed to an internal Priority Queue data structure.
- As soon as a connection finishes its work (or a client is released), the queue pops the item with the highest priority (lowest numerical value) and executes it immediately.
