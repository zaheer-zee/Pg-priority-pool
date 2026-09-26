const { Pool } = require('pg');
const { EventEmitter } = require('events');

class PriorityQueue {
  constructor(agingIntervalMs = 0) {
    this.items = [];
    this.agingIntervalMs = agingIntervalMs;
  }
  
  enqueue(item, priority) {
    this.items.push({
      item,
      originalPriority: priority,
      queuedAt: Date.now()
    });
  }

  dequeue() {
    if (this.isEmpty()) return null;

    let bestIndex = 0;
    let bestEffectivePriority = Infinity;
    const now = Date.now();

    for (let i = 0; i < this.items.length; i++) {
      const qItem = this.items[i];
      let effectivePriority = qItem.originalPriority;

      if (this.agingIntervalMs > 0) {
        const ageMs = now - qItem.queuedAt;
        const priorityBonus = Math.floor(ageMs / this.agingIntervalMs);
        effectivePriority -= priorityBonus;
      }

      if (effectivePriority < bestEffectivePriority) {
        bestEffectivePriority = effectivePriority;
        bestIndex = i;
      }
    }

    return this.items.splice(bestIndex, 1)[0].item;
  }

  get length() {
    return this.items.length;
  }

  isEmpty() {
    return this.items.length === 0;
  }
}

class PriorityPgPool extends EventEmitter {
  constructor(config = {}) {
    super();
    this.maxConnections = config.max || 10;
    this.maxQueueSize = config.maxQueueSize || 10000;
    this.pool = new Pool(config);
    this.queue = new PriorityQueue(config.agingIntervalMs || 0);
    this.activeCount = 0;

    this.pool.on('connect', (client) => this.emit('connect', client));
    this.pool.on('acquire', (client) => this.emit('acquire', client));
    this.pool.on('error', (err, client) => this.emit('error', err, client));
    this.pool.on('remove', (client) => this.emit('remove', client));
  }

  _parseArgs(text, params, callbackOrPriority, priorityParam) {
    let queryObj;
    let values;
    let cb;
    let priority = 10;

    if (typeof text === 'object') {
      queryObj = text;
      if (typeof params === 'function') {
        cb = params;
      } else if (typeof params === 'number') {
        priority = params;
      } else if (params !== undefined) {
        values = params;
        if (typeof callbackOrPriority === 'function') {
          cb = callbackOrPriority;
        } else if (typeof callbackOrPriority === 'number') {
          priority = callbackOrPriority;
        }
      }
    } else {
      queryObj = text;
      if (typeof params === 'function') {
        cb = params;
      } else if (typeof params === 'number') {
        priority = params;
      } else if (Array.isArray(params)) {
        values = params;
        if (typeof callbackOrPriority === 'function') {
          cb = callbackOrPriority;
        } else if (typeof callbackOrPriority === 'number') {
          priority = callbackOrPriority;
        } else if (typeof priorityParam === 'number') {
          priority = priorityParam;
        }
      }
    }

    return { queryObj, values, cb, priority };
  }

  query(text, params, callbackOrPriority, priorityParam) {
    const { queryObj, values, cb, priority } = this._parseArgs(text, params, callbackOrPriority, priorityParam);

    let promise;

    if (this.activeCount < this.maxConnections) {
      promise = this._execute(queryObj, values);
    } else if (this.queue.length >= this.maxQueueSize) {
      promise = Promise.reject(new Error('PriorityPgPool: Queue is full (maxQueueSize exceeded)'));
    } else {
      promise = new Promise((resolve, reject) => {
        this.queue.enqueue({
          type: 'query',
          queryObj,
          values,
          resolve,
          reject
        }, priority);
      });
    }

    if (cb) {
      promise
        .then(res => cb(null, res))
        .catch(err => cb(err, null));
      return;
    }

    return promise;
  }

  connect(callbackOrPriority) {
    let cb;
    let priority = 10;

    if (typeof callbackOrPriority === 'function') {
      cb = callbackOrPriority;
    } else if (typeof callbackOrPriority === 'number') {
      priority = callbackOrPriority;
    }

    let promise;

    if (this.activeCount < this.maxConnections) {
      promise = this._connectClient();
    } else if (this.queue.length >= this.maxQueueSize) {
      promise = Promise.reject(new Error('PriorityPgPool: Queue is full (maxQueueSize exceeded)'));
    } else {
      promise = new Promise((resolve, reject) => {
        this.queue.enqueue({
          type: 'connect',
          resolve,
          reject
        }, priority);
      });
    }

    if (cb) {
      promise
        .then(client => cb(null, client))
        .catch(err => cb(err, null));
      return;
    }

    return promise;
  }

  async _execute(queryObj, values) {
    this.activeCount++;
    try {
      const result = await this.pool.query(queryObj, values);
      return result;
    } finally {
      this.activeCount--;
      this._processNext();
    }
  }

  async _connectClient() {
    this.activeCount++;
    try {
      const client = await this.pool.connect();
      const originalRelease = client.release;
      let released = false;
      
      client.release = (err) => {
        if (released) return;
        released = true;
        originalRelease.call(client, err);
        this.activeCount--;
        this._processNext();
      };
      
      return client;
    } catch (err) {
      this.activeCount--;
      this._processNext();
      throw err;
    }
  }

  _processNext() {
    if (this.queue.isEmpty() || this.activeCount >= this.maxConnections) {
      return;
    }

    const nextTask = this.queue.dequeue();
    
    if (nextTask.type === 'query') {
      this._execute(nextTask.queryObj, nextTask.values)
        .then(nextTask.resolve)
        .catch(nextTask.reject);
    } else if (nextTask.type === 'connect') {
      this._connectClient()
        .then(nextTask.resolve)
        .catch(nextTask.reject);
    }
  }

  async end() {
    return this.pool.end();
  }

  get totalCount() {
    return this.pool.totalCount;
  }

  get idleCount() {
    return this.pool.idleCount;
  }

  get waitingCount() {
    return this.pool.waitingCount + this.queue.length;
  }
}

module.exports = {
  PriorityPgPool,
};
