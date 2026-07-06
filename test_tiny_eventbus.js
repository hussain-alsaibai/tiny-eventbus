'use strict';

/**
 * Tests for tiny-eventbus.
 * Run with: node test_tiny_eventbus.js
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const assert = require('assert');

const { TinyEventBus } = require('./tiny_eventbus');

let pass = 0;
let fail = 0;

function test(name, fn) {
  return Promise.resolve().then(fn).then(
    () => { pass++; console.log(`  ok  ${name}`); },
    (err) => {
      fail++;
      console.error(`  FAIL ${name}`);
      console.error(err && err.stack || err);
    }
  );
}

function tmp() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'tiny-eventbus-'));
}

(async () => {
  console.log('tiny-eventbus tests');

  await test('publish + subscribe receives event', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    let received = null;
    bus.subscribe('greeting', (ev) => { received = ev; });
    bus.publish('greeting', { msg: 'hi' });
    assert.ok(received, 'should have received');
    assert.strictEqual(received.topic, 'greeting');
    assert.deepStrictEqual(received.data, { msg: 'hi' });
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('event id and ts populated', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    let ev = null;
    bus.subscribe('x', (e) => { ev = e; });
    bus.publish('x', 1);
    assert.ok(ev.id, 'id set');
    assert.ok(ev.ts, 'ts set');
    assert.ok(Date.parse(ev.ts), 'ts is parseable');
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('multiple subscribers all receive', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    let a = 0, b = 0;
    bus.subscribe('x', () => a++);
    bus.subscribe('x', () => b++);
    bus.publish('x', 1);
    bus.publish('x', 2);
    assert.strictEqual(a, 2);
    assert.strictEqual(b, 2);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('replay on subscribe', async () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't' });
    bus.publish('a', 1);
    bus.publish('a', 2);
    bus.publish('a', 3);
    bus.close();

    // Reopen and subscribe - should get all three
    const bus2 = new TinyEventBus({ path: dir, name: 't' });
    const events = [];
    bus2.subscribe('a', (ev) => events.push(ev.data));
    // Give sync replay a tick
    await new Promise(r => setImmediate(r));
    assert.deepStrictEqual(events, [1, 2, 3]);
    bus2.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('survives restart', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't' });
    bus.publish('e', { n: 1 });
    bus.publish('e', { n: 2 });
    bus.close();
    const bus2 = new TinyEventBus({ path: dir, name: 't' });
    bus2.publish('e', { n: 3 });
    bus2.close();
    const bus3 = new TinyEventBus({ path: dir, name: 't' });
    const events = bus3.history({ topic: 'e' });
    assert.strictEqual(events.length, 3);
    assert.deepStrictEqual(events.map(e => e.data.n), [1, 2, 3]);
    bus3.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('unsubscribe stops receiving', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    let count = 0;
    const unsub = bus.subscribe('x', () => count++);
    bus.publish('x', 1);
    unsub();
    bus.publish('x', 2);
    assert.strictEqual(count, 1);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('wildcard subscribe receives all topics', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    const got = [];
    bus.subscribeAll((ev) => got.push(ev.topic));
    bus.publish('a', 1);
    bus.publish('b', 2);
    bus.publish('c.d', 3);
    assert.deepStrictEqual(got, ['a', 'b', 'c.d']);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('once resolves on first matching event', async () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    const p = bus.once('ping', null, 1000);
    bus.publish('ping', 'hello');
    const ev = await p;
    assert.strictEqual(ev.data, 'hello');
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('once with predicate filters events', async () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    const p = bus.once('v', (e) => e.data > 5, 1000);
    bus.publish('v', 1);
    bus.publish('v', 3);
    bus.publish('v', 10);
    const ev = await p;
    assert.strictEqual(ev.data, 10);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('once times out', async () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    let threw = false;
    try { await bus.once('v', null, 50); } catch (_) { threw = true; }
    assert.ok(threw);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('history filters by topic prefix', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't' });
    bus.publish('user.created', { id: 1 });
    bus.publish('user.updated', { id: 1 });
    bus.publish('order.created', { id: 9 });
    const userEvents = bus.history({ topicPrefix: 'user.' });
    assert.strictEqual(userEvents.length, 2);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('history respects limit', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't' });
    for (let i = 0; i < 10; i++) bus.publish('x', i);
    const evs = bus.history({ topic: 'x', limit: 3 });
    assert.strictEqual(evs.length, 3);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('subscriber error does not stop dispatch', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', replay: false });
    let reached = false;
    bus.subscribe('x', () => { throw new Error('boom'); });
    bus.subscribe('x', () => { reached = true; });
    bus.publish('x', 1);
    assert.ok(reached);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('rotation when log exceeds maxLogBytes', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't', maxLogBytes: 1024, replay: false });
    for (let i = 0; i < 100; i++) bus.publish('big', 'x'.repeat(50));
    bus.close();
    const files = fs.readdirSync(dir);
    assert.ok(files.some(f => f.includes('.log.jsonl.')), 'rotation file created');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('size returns event count', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't' });
    bus.publish('a', 1);
    bus.publish('a', 2);
    bus.publish('b', 3);
    assert.strictEqual(bus.size(), 3);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('endOffset returns current end', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't' });
    const o1 = bus.endOffset();
    bus.publish('a', 1);
    const o2 = bus.endOffset();
    assert.ok(o2 > o1);
    bus.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  await test('publish on closed bus throws', () => {
    const dir = tmp();
    const bus = new TinyEventBus({ path: dir, name: 't' });
    bus.close();
    let threw = false;
    try { bus.publish('x', 1); } catch (_) { threw = true; }
    assert.ok(threw);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail > 0 ? 1 : 0);
})();