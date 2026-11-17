import { bytes, clock, count, ms, rate, shardName, shortID } from "./format";

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
