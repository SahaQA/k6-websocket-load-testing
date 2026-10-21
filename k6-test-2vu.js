import ws from "k6/ws";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

// ─── Custom Metrics ────────────────────────────────────────────────
var messagesExchanged = new Counter("messages_exchanged");
var messageLatency = new Trend("message_latency_ms", true);
var pairingTime = new Trend("pairing_time_ms", true);

// ─── Scenario: 2 VUs, 100 messages per pair in 500ms ──────────────
export var options = {
  scenarios: {
    two_vu_chat: {
      executor: "shared-iterations",
      vus: 2,
      iterations: 2,
      maxDuration: "30s",
    },
  },
  thresholds: {
    messages_exchanged: ["count>=100"],
    message_latency_ms: ["p(95)<50"],
    pairing_time_ms: ["p(95)<2000"],
  },
};

var WS_URL = __ENV.WS_URL || "ws://localhost:8080";
var MESSAGES_PER_VU = 100;
var TARGET_DURATION_MS = 500;
var INTERVAL_MS = TARGET_DURATION_MS / MESSAGES_PER_VU; // 5ms

export default function () {
  var vuId = __VU;
  var userId = "k6-vu-" + vuId + "-" + Date.now();
  var openTime = Date.now();

  var res = ws.connect(WS_URL, {}, function (socket) {
    var paired = false;
    var sentCount = 0;
    var receivedAcks = 0;
    var receivedMessages = 0;
    var sendTimestamps = {};

    socket.on("open", function () {
      console.log("VU " + vuId + ": Connected, registering as " + userId);
      socket.send(
        JSON.stringify({ type: "register", userId: userId })
      );
    });

    socket.on("message", function (rawData) {
      var msg = JSON.parse(rawData);

      if (msg.type === "paired") {
        paired = true;
        pairingTime.add(Date.now() - openTime);
        console.log("VU " + vuId + ": Paired with " + msg.partnerId);

        // IMPORTANT: use socket.setInterval, NOT global setInterval
        socket.setInterval(function () {
          if (sentCount >= MESSAGES_PER_VU) {
            return;
          }
          sentCount++;
          sendTimestamps[sentCount] = Date.now();
          socket.send(
            JSON.stringify({
              type: "message",
              content: "msg-" + sentCount,
              seq: sentCount,
            })
          );
        }, INTERVAL_MS);
      }

      if (msg.type === "ack") {
        receivedAcks++;
        if (sendTimestamps[msg.seq]) {
          messageLatency.add(Date.now() - sendTimestamps[msg.seq]);
          delete sendTimestamps[msg.seq];
        }
        messagesExchanged.add(1);
      }

      if (msg.type === "message") {
        receivedMessages++;
      }

      if (msg.type === "waiting") {
        console.log("VU " + vuId + ": Waiting for partner...");
      }

      // Close when fully done
      if (receivedAcks >= MESSAGES_PER_VU && receivedMessages >= MESSAGES_PER_VU) {
        console.log(
          "VU " + vuId + ": Done. Sent=" + sentCount +
          " Acks=" + receivedAcks + " Recv=" + receivedMessages
        );
        socket.close();
      }
    });

    // Safety timeout
    socket.setTimeout(function () {
      console.log(
        "VU " + vuId + ": TIMEOUT. Sent=" + sentCount +
        " Acks=" + receivedAcks + " Recv=" + receivedMessages
      );
      socket.close();
    }, 15000);
  });

  check(res, {
    "WebSocket status is 101": function (r) {
      return r && r.status === 101;
    },
  });
}
