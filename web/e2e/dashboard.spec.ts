// Phase 9 gate, in the browser: killing a node in Compose shows on the
// dashboard within one poll interval, and the events page lists the
// election that replaces it, its dead verdict and the re-replication of its
// replicas, in that order.
import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import type { EventView, Overview } from "../src/api";

const compose = process.env.DKV_COMPOSE;

test("a killed node shows on the dashboard, then the events in order", async ({ page, request }) => {
  test.skip(!compose, "DKV_COMPOSE names the docker compose command of the cluster under test");
  await page.goto("/");
  await expect(page.getByTestId("stream-state")).toContainText("live", { timeout: 60_000 });
  await expect(page.getByTestId("nodes-up")).toHaveText("4 / 4", { timeout: 120_000 });

  // The victim: the node leading shard 0, once every group has a leader.
  let ov!: Overview;
  await expect(async () => {
    ov = (await (await request.get("/api/overview")).json()) as Overview;
    expect(ov.shards.filter((s) => s.leader !== "")).toHaveLength(ov.shards.length);
  }).toPass({ timeout: 60_000 });
  const leaderID = ov.shards.find((s) => s.shard === 0)!.leader;
  const victim = ov.nodes.find((n) => n.node_id === leaderID)!;
  const service = victim.addr.split(":")[0];
  const led = new Set(ov.shards.filter((s) => s.leader === leaderID).map((s) => s.shard));
  const card = page.getByTestId(`node-${victim.addr}`);
  await expect(card.getByText("up", { exact: true })).toBeVisible();

  execSync(`${compose} kill -s SIGKILL ${service}`, { stdio: "inherit" });
  const killed = Date.now();
  await expect(card.getByText("down", { exact: true })).toBeVisible({ timeout: 30_000 });
  const shownAfter = Date.now() - killed;
  console.log(`${service} (${leaderID}) killed; the dashboard showed it down ${shownAfter} ms later`);
  // One poll interval, the poll's timeout (800 ms), and a second for the
  // stream and the browser.
  expect(shownAfter).toBeLessThan(ov.poll_interval_ms + 800 + 1000);

  // The events, in time order: a new leader for a shard the victim led,
  // then the meta leader's dead verdict, then a replica added elsewhere.
  let order: string[] = [];
  await expect(async () => {
    const events = (await (await request.get("/api/events?limit=5000")).json()) as EventView[];
    const after = events.filter((e) => Date.parse(e.time) >= killed - 1000);
    const elected = after.findIndex((e) => e.kind === "leader_elected" && e.shard !== null && led.has(e.shard) && e.reporter !== leaderID);
    const dead = after.findIndex((e) => e.kind === "node_dead" && e.node === leaderID);
    const added = after.findIndex((e, i) => e.kind === "replica_added" && dead >= 0 && i > dead);
    expect([elected, dead, added].every((i) => i >= 0)).toBe(true);
    expect(elected).toBeLessThan(dead);
    expect(dead).toBeLessThan(added);
    order = [after[elected], after[dead], after[added]].map((e) => `${e.time} ${e.kind}`);
  }).toPass({ timeout: 120_000 });
  console.log(`events: ${order.join(", ")}`);

  await page.getByRole("link", { name: "Events", exact: true }).click();
  for (const kind of ["leader_elected", "node_dead", "replica_added"]) {
    await expect(page.locator(`tr[data-kind="${kind}"]`).first()).toBeVisible();
  }
});
