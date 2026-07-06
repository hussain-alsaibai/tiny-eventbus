# tiny-eventbus

> Zero-dependency durable event bus with replay. JSONL log + EventEmitter +
> topic filtering. For agents that need pub/sub that survives a restart.

```bash
npm install tiny-eventbus
```

## Why?

Most "event bus" libraries assume you already have Kafka / Redis / NATS
running. Sometimes you just want:

```js
const bus = new TinyEventBus({ path: '~/.myagent/events' });
bus.subscribe('scan.complete', (ev) => log(ev.data));
bus.publish('scan.complete', { found: 12 });
```

…and have those events still be there when the next cron run picks up
30 minutes later.

That's what this is. One file, stdlib only, JSONL on disk, `EventEmitter`
in memory, automatic replay on subscribe.

## What's NOT here (intentional)

- Multi-process / network access (use NATS, Redis Streams, Kafka)
- Strong delivery guarantees (crash mid-publish = lost event in the gap
  between `write` and `sync`; call `bus.sync()` if you need harder guarantees)
- Schema registry / Avro / protobuf
- Acknowledgements / dead-letter queues
- Per-partition ordering

If you need any of those, use the real thing. `tiny-eventbus` is the
"fits in a cron job, must not lose state" tier.

## Usage

### Pub/Sub

```js
const { TinyEventBus } = require('tiny-eventbus');

const bus = new TinyEventBus({ path: './data/events', name: 'main' });

// Subscribe to one topic
const unsub = bus.subscribe('user.signup', (event) => {
  console.log('new user:', event.data);
  console.log('  at:', event.ts, 'id:', event.id);
});

// Subscribe to everything
bus.subscribeAll((event) => {
  console.log('event:', event.topic, event.data);
});

// Publish
bus.publish('user.signup', { email: 'a@b.com', plan: 'pro' });

// Wildcards work in topic names with dot-separated namespaces
bus.publish('user.signup.completed', { userId: 42 });
bus.publish('order.created', { total: 99 });

// Done
unsub();
bus.close();
```

### Replay on restart

```js
// First run
const bus1 = new TinyEventBus({ path: './events' });
bus1.publish('scan.complete', { found: 12 });
bus1.close();

// Second run (later that day, in a fresh process)
const bus2 = new TinyEventBus({ path: './events' });
const events = [];
bus2.subscribe('scan.complete', (ev) => events.push(ev));
// events now contains { found: 12 }
```

### Awaitable one-shot

```js
// Wait for the next event matching a predicate
const event = await bus.once('deploy.complete',
  (ev) => ev.data.environment === 'production',
  30_000  // timeout in ms
);
console.log('deployed to prod:', event.data);
```

### Reading history without subscribing

```js
const events = bus.history({
  topic: 'user.signup',
  limit: 100,
});

const userEvents = bus.history({ topicPrefix: 'user.' });
const recentErrors = bus.history({ topic: 'error', limit: 50 });

// Resume from a specific offset (useful for batch processors)
const cursor = loadCursorFromDisk();
const newEvents = bus.history({ fromOffset: cursor, limit: 1000 });
```

### Rotation

The log rotates automatically when it exceeds `maxLogBytes` (default 10 MB).
Rotated files are named `<bus>.log.jsonl.<timestamp>` and live alongside the
live log. Replay currently reads only the live log; if you need full history,
include the rotated files yourself.

## API

### `new TinyEventBus(opts)`

| Option | Type | Default | Purpose |
|---|---|---|---|
| `path` | string | required | Directory for the log |
| `name` | string | `'bus'` | Log filename prefix |
| `replay` | bool | `true` | Replay history on subscribe |
| `maxLogBytes` | number | `10 MB` | Rotate when log exceeds this |

### Methods

- `publish(topic, data, extra?)` → event id
- `subscribe(topic, callback, opts?)` → unsubscribe function
- `subscribeAll(callback, opts?)` → unsubscribe function
- `once(topic, predicate?, timeoutMs?)` → Promise<event>
- `history(filter?)` → `Event[]`
- `size()` → number of events on disk
- `endOffset()` → byte offset for replay cursors
- `close()`

### Event shape

```ts
{
  id: string;          // generated
  ts: string;          // ISO timestamp
  topic: string;       // e.g. "user.signup"
  data: any;           // JSON-serializable payload
  // ...extra fields from publish(topic, data, extra)
}
```

## Why JSONL?

- Append-only writes are atomic on POSIX
- `tail -f` works for debugging
- `grep` works for ad-hoc queries
- One line per event = trivial to recover from corruption (skip bad lines)
- You can `cat` the file and understand it

## Tests

```bash
node test_tiny_eventbus.js
```

17 tests cover: pub/sub, multi-subscriber, replay, restart survival,
unsubscribe, wildcard, `once` with predicate + timeout, history filtering,
limit, error isolation, log rotation, size, endOffset, closed-bus errors.

## License

MIT