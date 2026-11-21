import { ago, bytes, clock, count, kindLabel, ms, msValue, rate, rateValue, shardName, shortID } from "./format";

test("formats values without changing them", () => {
  expect(bytes(0)).toBe("0 B");
  expect(bytes(1023)).toBe("1023 B");
  expect(bytes(1536)).toBe("1.5 KiB");
  expect(bytes(5 * 1024 * 1024)).toBe("5.0 MiB");
  expect(rate(0)).toBe("0.0/s");
  expect(rate(3.25)).toBe("3.3/s");
  expect(rate(1234.4)).toBe("1,234/s");
  expect(ms(null)).toBe("–");
  expect(ms(1.234)).toBe("1.23 ms");
  expect(ms(250)).toBe("250 ms");
  expect(count(1234567)).toBe("1,234,567");
  expect(shardName(-1)).toBe("meta");
  expect(shardName(3)).toBe("3");
  expect(shardName(null)).toBe("–");
  expect(shortID("0123456789abcdef")).toBe("01234567");
  expect(clock("2026-11-17T20:40:01.123456789Z")).toBe("20:40:01.123Z");
});

test("split figures read the same as the whole ones", () => {
  for (const n of [0, 3.25, 9.99, 10, 1234.4]) expect(`${rateValue(n)}/s`).toBe(rate(n));
  for (const n of [1.234, 250]) expect(`${msValue(n)} ms`).toBe(ms(n));
  expect(msValue(null)).toBe(ms(null));
});

test("relative times", () => {
  const now = Date.parse("2026-11-21T12:00:00Z");
  expect(ago("2026-11-21T12:00:03Z", now)).toBe("just now"); // a little ahead: clock skew
  expect(ago("2026-11-21T11:59:58Z", now)).toBe("just now");
  expect(ago("2026-11-21T11:59:18Z", now)).toBe("42s ago");
  expect(ago("2026-11-21T11:55:00Z", now)).toBe("5m ago");
  expect(ago("2026-11-21T09:00:00Z", now)).toBe("3h ago");
  expect(ago("2026-11-19T12:00:00Z", now)).toBe("2d ago");
  expect(ago("not a time", now)).toBe("–");
});

test("event kinds in words", () => {
  expect(kindLabel("leader_elected")).toBe("Leader elected");
  expect(kindLabel("node_dead")).toBe("Node dead");
});
