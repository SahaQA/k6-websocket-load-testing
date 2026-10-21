const WebSocket = require("ws");

// ─── Configuration ─────────────────────────────────────────────────
const WS_URL = process.env.WS_URL || "ws://localhost:8080";
const TOTAL_VUS = parseInt(process.env.VUS || "2", 10);
const MESSAGES_PER_VU = 100;
const TARGET_DURATION_MS = 500;
const INTERVAL_MS = TARGET_DURATION_MS / MESSAGES_PER_VU; // 5ms

// ─── Metrics Collection ────────────────────────────────────────────
const metrics = {
  totalMessagesSent: 0,
  totalAcksReceived: 0,
  totalMessagesReceived: 0,
  pairCount: 0,
  latencies: [],
  pairingTimes: [],
  errors: [],
  vuResults: [],
};

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

// ─── Single VU Simulation ──────────────────────────────────────────
function runVU(vuId) {
  return new Promise((resolve) => {
    const userId = `node-vu-${vuId}-${Date.now()}`;
    const openTime = Date.now();
    const sendTimestamps = {};
    let sentCount = 0;
    let receivedAcks = 0;
    let receivedMessages = 0;
    let paired = false;
    let partnerId = null;
    let sendStartTime = null;
    let sendEndTime = null;

    const ws = new WebSocket(WS_URL);

    ws.on("open", () => {
      ws.send(JSON.stringify({ type: "register", userId }));
    });

    ws.on("message", (rawData) => {
      const msg = JSON.parse(rawData.toString());

      switch (msg.type) {
        case "paired": {
          paired = true;
          partnerId = msg.partnerId;
          const pairingTime = Date.now() - openTime;
          metrics.pairingTimes.push(pairingTime);
          metrics.pairCount++;
          console.log(
            `  [VU ${vuId}] Paired with ${partnerId} (${pairingTime}ms)`
          );

          // Start sending messages
          sendStartTime = Date.now();
          const interval = setInterval(() => {
            if (sentCount >= MESSAGES_PER_VU) {
              sendEndTime = Date.now();
              clearInterval(interval);
              return;
            }
            sentCount++;
            metrics.totalMessagesSent++;
            const seq = sentCount;
            sendTimestamps[seq] = Date.now();

            ws.send(
              JSON.stringify({
                type: "message",
                content: `Load test msg #${seq} from VU-${vuId}`,
                seq,
              })
            );
          }, INTERVAL_MS);
          break;
        }

        case "ack": {
          receivedAcks++;
          metrics.totalAcksReceived++;
          if (sendTimestamps[msg.seq]) {
            metrics.latencies.push(Date.now() - sendTimestamps[msg.seq]);
            delete sendTimestamps[msg.seq];
          }
          checkDone();
          break;
        }

        case "message": {
          receivedMessages++;
          metrics.totalMessagesReceived++;
          checkDone();
          break;
        }

        case "waiting":
          console.log(`  [VU ${vuId}] Waiting for partner...`);
          break;

        case "partner_disconnected":
          console.log(`  [VU ${vuId}] Partner disconnected`);
          finish();
          break;
      }
    });

    ws.on("error", (err) => {
      metrics.errors.push(`VU ${vuId}: ${err.message}`);
      finish();
    });

    function checkDone() {
      if (
        receivedAcks >= MESSAGES_PER_VU &&
        receivedMessages >= MESSAGES_PER_VU
      ) {
        finish();
      }
    }

    // Safety timeout
    const timeout = setTimeout(() => {
      console.log(
        `  [VU ${vuId}] TIMEOUT. Sent=${sentCount}, Acks=${receivedAcks}, Recv=${receivedMessages}`
      );
      finish();
    }, 30000);

    let finished = false;
    function finish() {
      if (finished) return;
      finished = true;
      clearTimeout(timeout);

      const result = {
        vuId,
        userId,
        paired,
        partnerId,
        sentCount,
        receivedAcks,
        receivedMessages,
        sendDuration: sendEndTime ? sendEndTime - sendStartTime : null,
      };
      metrics.vuResults.push(result);

      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
      resolve(result);
    }
  });
}

// ─── Main ──────────────────────────────────────────────────────────
async function main() {
  console.log("═══════════════════════════════════════════════════════════");
  console.log(`  WebSocket Load Test — ${TOTAL_VUS} VUs, ${MESSAGES_PER_VU} msgs/VU, ${TARGET_DURATION_MS}ms target`);
  console.log("═══════════════════════════════════════════════════════════\n");

  const testStart = Date.now();

  // Launch all VUs concurrently
  const vuPromises = [];
  for (let i = 1; i <= TOTAL_VUS; i++) {
    vuPromises.push(runVU(i));
  }

  const results = await Promise.all(vuPromises);
  const totalTime = Date.now() - testStart;

  // ─── Report ────────────────────────────────────────────────────
  console.log("\n═══════════════════════════════════════════════════════════");
  console.log("  RESULTS");
  console.log("═══════════════════════════════════════════════════════════\n");

  // Per-VU summary
  console.log("─── Per-VU Summary ───────────────────────────────────────");
  for (const r of results) {
    const status =
      r.sentCount >= MESSAGES_PER_VU && r.receivedAcks >= MESSAGES_PER_VU
        ? "✅ PASS"
        : "❌ FAIL";
    console.log(
      `  VU ${r.vuId}: ${status} | Sent: ${r.sentCount} | Acks: ${r.receivedAcks} | Recv: ${r.receivedMessages} | Send duration: ${r.sendDuration}ms`
    );
  }

  // Aggregate metrics
  console.log("\n─── Aggregate Metrics ────────────────────────────────────");
  console.log(`  Total VUs:              ${TOTAL_VUS}`);
  console.log(`  Pairs formed:           ${metrics.pairCount}`);
  console.log(`  Total messages sent:    ${metrics.totalMessagesSent}`);
  console.log(`  Total acks received:    ${metrics.totalAcksReceived}`);
  console.log(`  Total messages received:${metrics.totalMessagesReceived}`);
  console.log(`  Total test duration:    ${totalTime}ms`);

  if (metrics.latencies.length > 0) {
    const avgLat =
      metrics.latencies.reduce((a, b) => a + b, 0) / metrics.latencies.length;
    console.log(`\n─── Latency (message round-trip) ─────────────────────────`);
    console.log(`  Min:    ${Math.min(...metrics.latencies)}ms`);
    console.log(`  Avg:    ${avgLat.toFixed(2)}ms`);
    console.log(`  p50:    ${percentile(metrics.latencies, 50)}ms`);
    console.log(`  p90:    ${percentile(metrics.latencies, 90)}ms`);
    console.log(`  p95:    ${percentile(metrics.latencies, 95)}ms`);
    console.log(`  p99:    ${percentile(metrics.latencies, 99)}ms`);
    console.log(`  Max:    ${Math.max(...metrics.latencies)}ms`);
  }

  if (metrics.pairingTimes.length > 0) {
    const avgPair =
      metrics.pairingTimes.reduce((a, b) => a + b, 0) /
      metrics.pairingTimes.length;
    console.log(`\n─── Pairing Time ─────────────────────────────────────────`);
    console.log(`  Min:    ${Math.min(...metrics.pairingTimes)}ms`);
    console.log(`  Avg:    ${avgPair.toFixed(2)}ms`);
    console.log(`  Max:    ${Math.max(...metrics.pairingTimes)}ms`);
  }

  // Throughput
  const expectedPairs = TOTAL_VUS / 2;
  const expectedMessages = expectedPairs * MESSAGES_PER_VU;
  const throughput = (metrics.totalAcksReceived / (totalTime / 1000)).toFixed(2);
  console.log(`\n─── Throughput ───────────────────────────────────────────`);
  console.log(`  Messages/sec:           ${throughput}`);
  console.log(`  Expected total msgs:    ${expectedMessages}`);
  console.log(`  Actual delivered:       ${metrics.totalAcksReceived}`);

  if (metrics.errors.length > 0) {
    console.log(`\n─── Errors ───────────────────────────────────────────────`);
    metrics.errors.forEach((e) => console.log(`  ${e}`));
  }

  // Thresholds check
  console.log("\n─── Threshold Checks ─────────────────────────────────────");
  const p95Lat = percentile(metrics.latencies, 95);
  const allComplete = results.every(
    (r) => r.sentCount >= MESSAGES_PER_VU && r.receivedAcks >= MESSAGES_PER_VU
  );

  const checks = [
    {
      name: `Messages exchanged >= ${expectedMessages}`,
      pass: metrics.totalAcksReceived >= expectedMessages,
      actual: metrics.totalAcksReceived,
    },
    {
      name: "p95 latency < 100ms",
      pass: p95Lat < 100,
      actual: `${p95Lat}ms`,
    },
    {
      name: "All VUs completed successfully",
      pass: allComplete,
      actual: `${results.filter((r) => r.receivedAcks >= MESSAGES_PER_VU).length}/${TOTAL_VUS}`,
    },
  ];

  let allPassed = true;
  for (const c of checks) {
    const icon = c.pass ? "✅" : "❌";
    console.log(`  ${icon} ${c.name} (actual: ${c.actual})`);
    if (!c.pass) allPassed = false;
  }

  console.log(
    `\n═══════════════════════════════════════════════════════════`
  );
  console.log(allPassed ? "  ✅ ALL THRESHOLDS PASSED" : "  ❌ SOME THRESHOLDS FAILED");
  console.log(
    "═══════════════════════════════════════════════════════════\n"
  );

  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
