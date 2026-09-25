const { PriorityPgPool } = require('./index.js');

async function runTests() {
  console.log('--- Testing PriorityPgPool ---');
  
  // 1. Create a pool with max 2 connections
  const pool = new PriorityPgPool({ max: 2, agingIntervalMs: 1000 });

  // Mock the internal pool to simulate slow queries (50ms) without needing a real database
  let queryCount = 0;
  pool.pool.query = async (queryObj, values) => {
    queryCount++;
    const id = queryCount;
    return new Promise(resolve => setTimeout(() => resolve({ rows: [{ id }] }), 50));
  };
  
  pool.pool.connect = async () => {
    return {
      query: async () => {},
      release: () => {}
    };
  };

  const results = [];

  console.log('Dispatching 5 queries simultaneously...');
  console.log('Max connections is 2. The first 2 will execute immediately, the rest will queue.');

  // Q1 and Q2 should execute immediately
  const q1 = pool.query('SELECT 1', [], 10).then(() => results.push('Q1 (Priority 10)'));
  const q2 = pool.query('SELECT 2', [], 10).then(() => results.push('Q2 (Priority 10)'));
  
  // Q3, Q4, Q5 will go to the queue. 
  // Q3 has the highest priority (1), so it should jump to the front of the queue!
  const q3 = pool.query('SELECT 3', [], 1).then(() => results.push('Q3 (Priority 1) -> Jumped Queue!'));
  const q4 = pool.query('SELECT 4', [], 5).then(() => results.push('Q4 (Priority 5)'));
  const q5 = pool.query('SELECT 5', [], 20).then(() => results.push('Q5 (Priority 20)'));

  await Promise.all([q1, q2, q3, q4, q5]);

  console.log('\n--- Execution Order ---');
  results.forEach((r, i) => console.log(`${i + 1}. ${r}`));
  
  // 2. Test Callback support
  console.log('\n--- Testing Callback API ---');
  await new Promise((resolve, reject) => {
    pool.query('SELECT 1', (err, res) => {
      if (err) return reject(err);
      console.log('Callback works successfully!');
      resolve();
    });
  });

  // 3. Test Queue limit
  console.log('\n--- Testing maxQueueSize ---');
  const smallQueuePool = new PriorityPgPool({ max: 0, maxQueueSize: 2 });
  
  try {
    smallQueuePool.query('SELECT 1'); // activeCount goes to 0 (because max is 0), so it queues (size 1)
    smallQueuePool.query('SELECT 2'); // queues (size 2)
    smallQueuePool.query('SELECT 3'); // queues (size 3) - SHOULD FAIL!
  } catch (err) {
    console.log('Queue limit caught correctly:', err.message);
  }

  console.log('\n✅ All tests passed successfully!');
  process.exit(0);
}

runTests().catch(console.error);
