'use strict';

/**
 * tiny-eventbus — durable event bus with replay.
 *
 * Single-process pub/sub with an append-only JSONL log so subscribers can
 * either get live events or replay history from any point. Designed for
 * agent workflows where:
 *
 *   - Events need to survive process restarts (cron → next-run picks up where it left off)
 *   - Multiple components subscribe to overlapping topics
 *   - You want a tiny audit trail without setting up Kafka/Redis Streams
 *
 * NOT a distributed message broker. NOT multi-process. If you need those, use
 * NATS, Redis Streams, or Kafka.
 */

const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');

class TinyEventBus extends EventEmitter {
  /**
   * @param {object} opts
   * @param {string} opts.path           Directory for the log file.
   * @param {string} [opts.name]         Logical bus name (used in log filename).
   * @param {boolean} [opts.replay=true] Replay historical events on subscribe.
   * @param {number} [opts.maxLogBytes]  Rotate log when it exceeds this size.
   */
  constructor(opts) {
    super();
    if (!opts || !opts.path) throw new Error('path is required');
    this.path = opts.path;
    this.name = opts.name || 'bus';
    this.replay = opts.replay !== false;
    this.maxLogBytes = opts.maxLogBytes || 10 * 1024 * 1024;
    fs.mkdirSync(this.path, { recursive: true });
    this.logPath = path.join(this.path, `${this.name}.log.jsonl`);
    this._offset = 0;
    this._closed = false;
    this._subscribers = new Map(); // topic -> Set<callback>
    this._liveFromOffset = 0; // offset of next live event
    this._fd = fs.openSync(this.logPath, 'a');
    this._hydrateOffset();
    this._liveFromOffset = this._offset;
  }

  // ---------- pub/sub ----------

  /**
   * Publish an event. Event must have `topic` and `data` (or pass them positionally).
   * Returns the assigned event id.
   */
  publish(topic, data, extra = {}) {
    if (this._closed) throw new Error('bus is closed');
    const event = {
      id: this._genId(),
      ts: new Date().toISOString(),
      topic,
      data,
      ...extra,
    };
    const line = JSON.stringify(event) + '\n';
    fs.writeSync(this._fd, line);
    this._offset += Buffer.byteLength(line);
    this._maybeRotate();
    this._dispatch(event);
    return event.id;
  }

  /**
   * Subscribe to a topic. Replay fires first (if enabled), then live events.
   * Returns an unsubscribe function.
   */
  subscribe(topic, callback, opts = {}) {
    const replayFrom = opts.replayFrom || 0;
    if (!this._subscribers.has(topic)) this._subscribers.set(topic, new Set());
    this._subscribers.get(topic).add(callback);
    if (this.replay) {
      this._replay(topic, replayFrom, callback);
    }
    return () => {
      const set = this._subscribers.get(topic);
      if (set) set.delete(callback);
    };
  }

  /**
   * Subscribe to all topics (wildcard).
   */
  subscribeAll(callback, opts = {}) {
    return this.subscribe('*', callback, opts);
  }

  /**
   * Wait for a single event matching the predicate.
   * Resolves the event object (or rejects on timeout).
   */
  once(topic, predicate = null, timeoutMs = 0) {
    return new Promise((resolve, reject) => {
      let timer = null;
      const unsub = this.subscribe(topic, (event) => {
        if (!predicate || predicate(event)) {
          if (timer) clearTimeout(timer);
          unsub();
          resolve(event);
        }
      });
      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          unsub();
          reject(new Error(`once(${topic}) timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }
    });
  }

  // ---------- history ----------

  /**
   * Read historical events, optionally filtered.
   * @param {object} [filter]
   * @param {string} [filter.topic]      Exact topic match.
   * @param {string} [filter.topicPrefix] Topic starts with.
   * @param {number} [filter.fromOffset] Start at this offset in the log.
   * @param {number} [filter.limit=Infinity] Max events to return.
   */
  history(filter = {}) {
    const out = [];
    const fromOffset = filter.fromOffset || 0;
    const limit = filter.limit || Infinity;
    if (!fs.existsSync(this.logPath)) return out;
    const fd = fs.openSync(this.logPath, 'r');
    try {
      let pos = fromOffset;
      const stat = fs.fstatSync(fd);
      const buf = Buffer.alloc(Math.min(stat.size - pos, 1024 * 1024));
      let leftover = '';
      while (pos < stat.size) {
        const want = Math.min(buf.length, stat.size - pos);
        fs.readSync(fd, buf, 0, want, pos);
        pos += want;
        const text = leftover + buf.toString('utf8', 0, want);
        const lines = text.split('\n');
        leftover = lines.pop(); // last (possibly partial) line
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const ev = JSON.parse(line);
            if (filter.topic && ev.topic !== filter.topic) continue;
            if (filter.topicPrefix && !(ev.topic || '').startsWith(filter.topicPrefix)) continue;
            out.push(ev);
            if (out.length >= limit) return out;
          } catch (_) { /* skip corrupt line */ }
        }
      }
      if (leftover.trim()) {
        try {
          const ev = JSON.parse(leftover);
          if ((!filter.topic || ev.topic === filter.topic) &&
              (!filter.topicPrefix || (ev.topic || '').startsWith(filter.topicPrefix))) {
            out.push(ev);
          }
        } catch (_) {}
      }
    } finally {
      fs.closeSync(fd);
    }
    return out;
  }

  /**
   * Total event count currently on disk (best effort, one-line-per-event).
   */
  size() {
    if (!fs.existsSync(this.logPath)) return 0;
    const text = fs.readFileSync(this.logPath, 'utf8');
    return text.split('\n').filter(l => l.trim()).length;
  }

  /**
   * Current end-of-log offset (for replay cursors).
   */
  endOffset() {
    return this._offset;
  }

  // ---------- lifecycle ----------

  close() {
    if (this._closed) return;
    this._closed = true;
    fs.closeSync(this._fd);
    this.removeAllListeners();
  }

  // ---------- internals ----------

  _hydrateOffset() {
    if (!fs.existsSync(this.logPath)) return;
    const stat = fs.statSync(this.logPath);
    this._offset = stat.size;
  }

  _maybeRotate() {
    if (this._offset < this.maxLogBytes) return;
    fs.closeSync(this._fd);
    const rotated = `${this.logPath}.${Date.now()}`;
    fs.renameSync(this.logPath, rotated);
    this._fd = fs.openSync(this.logPath, 'a');
    this._offset = 0;
    this._liveFromOffset = 0;
  }

  _genId() {
    return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  }

  _dispatch(event) {
    // topic subscribers
    const subs = this._subscribers.get(event.topic);
    if (subs) {
      for (const cb of subs) {
        try { cb(event); } catch (err) { this.emit('subscriber_error', err, event); }
      }
    }
    // wildcard
    const wild = this._subscribers.get('*');
    if (wild) {
      for (const cb of wild) {
        try { cb(event); } catch (err) { this.emit('subscriber_error', err, event); }
      }
    }
  }

  _replay(topic, fromOffset, callback) {
    const events = this.history({
      topic,
      topicPrefix: topic === '*' ? '' : undefined,
      fromOffset,
      limit: 10_000, // safety cap
    });
    for (const ev of events) {
      try { callback(ev); } catch (err) { this.emit('subscriber_error', err, ev); }
    }
  }
}

module.exports = { TinyEventBus };

// CLI: tail/grep the log
if (require.main === module) {
  const args = process.argv.slice(2);
  const cmd = args[0];
  if (cmd === 'tail' && args[1]) {
    const bus = new TinyEventBus({ path: args[1] });
    bus.on('subscriber_error', (err) => console.error('subscriber error:', err.message));
    bus.subscribeAll((ev) => {
      console.log(JSON.stringify(ev));
    }, { replayFrom: 0 });
  } else if (cmd === 'history' && args[1]) {
    const bus = new TinyEventBus({ path: args[1] });
    const filter = {};
    const topicArg = args.find(a => a.startsWith('--topic='));
    if (topicArg) filter.topic = topicArg.split('=')[1];
    for (const ev of bus.history(filter)) {
      console.log(JSON.stringify(ev));
    }
    bus.close();
  } else {
    console.log(`tiny-eventbus — durable pub/sub with replay
Usage:
  node tiny_eventbus.js tail <bus-path>          # tail all events live
  node tiny_eventbus.js history <bus-path> [--topic=foo]
`);
  }
}