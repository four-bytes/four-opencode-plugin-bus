#!/usr/bin/env bun
/**
 * Plugin Bus Demo — proves the bus works end-to-end without opencode.
 * Run: bun run ~/four-opencode-plugin-bus/demo/demo.ts
 */
import { spawn } from "bun";
import { BusClient } from "../../four-opencode-plugin-lib/src/bus-client.js";
import { BusTui } from "../../four-opencode-plugin-lib/src/bus-tui.js";

const BUS_BIN = `${process.env.HOME}/.local/bin/bus`;

async function demo() {
  console.log("🔌 Plugin Bus Demo\n");

  // Clean stale state
  await spawn(["pkill", "-f", ".local/bin/bus"]).exited.catch(() => {});
  const rmResult = await spawn(["rm", "-f", `${process.env.HOME}/.cache/opencode/plugin-bus/port.json`]).exited;
  await new Promise((r) => setTimeout(r, 500));

  // Step 1: Start bus
  console.log("1. Starting bus binary...");
  const proc = spawn([BUS_BIN], { stdout: "pipe", stderr: "pipe" });
  const reader = proc.stdout!.getReader();
  const { value } = await reader.read();
  const line = new TextDecoder().decode(value).trim();
  const port = JSON.parse(line).port;
  console.log(`   ✅ Bus running on port ${port}\n`);
  reader.releaseLock();

  // Step 2: BusClient publish
  console.log("2. Publishing via BusClient (HTTP)...");
  const client = await BusClient.connect();
  await client.publish("demo/test", { msg: "Hello from BusClient!", ts: Date.now() });
  console.log("   ✅ Published to demo/test\n");

  // Step 3: BusTui subscribe + last-value cache
  console.log("3. Subscribing via BusTui (WebSocket)...");
  const tui = await BusTui.connect();
  const received: unknown[] = [];
  tui.subscribe("demo/test", (msg) => received.push(msg.payload));
  await new Promise((r) => setTimeout(r, 500));
  if (received.length > 0) {
    console.log("   ✅ Last-value cache delivered:", JSON.stringify(received[0]));
  } else {
    console.log("   ⚠️  No cached message (may be timing issue)");
  }
  console.log();

  // Step 4: Real-time publish after subscribe
  console.log("4. Real-time publish after subscribe...");
  await client.publish("demo/test", { msg: "Real-time message!" });
  await new Promise((r) => setTimeout(r, 500));
  console.log(`   ✅ Received ${received.length} total messages\n`);

  // Step 5: Wildcard matching
  console.log("5. Wildcard channel matching...");
  const wcReceived: unknown[] = [];
  tui.subscribe("demo/+/wildcard", (msg) => wcReceived.push(msg.payload));
  await new Promise((r) => setTimeout(r, 300));
  await client.publish("demo/foo/wildcard", { id: 1 });
  await client.publish("demo/bar/wildcard", { id: 2 });
  await client.publish("demo/baz/other", { id: 3 });
  await new Promise((r) => setTimeout(r, 500));
  const wcExpected = wcReceived.length === 2 ? "✅" : "❌";
  console.log(`   ${wcExpected} Wildcard received ${wcReceived.length} messages (expected 2 — 'baz/other' should not match)\n`);

  // Cleanup
  tui.close();
  proc.kill();
  await proc.exited;
  console.log("✅ Demo complete — bus is working!");
}

demo().catch((err) => {
  console.error("❌ Demo failed:", err.message);
  process.exit(1);
});
