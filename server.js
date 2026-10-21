const { WebSocketServer } = require("ws");

const PORT = 8080;
const wss = new WebSocketServer({ port: PORT });

// Track connected users: Map<userId, ws>
const users = new Map();
// Track paired conversations: Map<odifieduserId, partnerUserId>
const pairs = new Map();
// Queue of users waiting to be paired
const waitingQueue = [];

// Stats
let totalMessages = 0;
let totalConnections = 0;

wss.on("connection", (ws) => {
  let userId = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      ws.send(JSON.stringify({ type: "error", message: "Invalid JSON" }));
      return;
    }

    switch (msg.type) {
      case "register": {
        userId = msg.userId;
        users.set(userId, ws);
        totalConnections++;

        // Try to pair with someone in the waiting queue
        if (waitingQueue.length > 0) {
          const partnerId = waitingQueue.shift();
          const partnerWs = users.get(partnerId);

          if (partnerWs && partnerWs.readyState === 1) {
            // Pair them
            pairs.set(userId, partnerId);
            pairs.set(partnerId, userId);

            ws.send(
              JSON.stringify({
                type: "paired",
                partnerId,
              })
            );
            partnerWs.send(
              JSON.stringify({
                type: "paired",
                partnerId: userId,
              })
            );

            console.log(`Paired: ${userId} <-> ${partnerId}`);
          } else {
            // Partner disconnected, add self to queue
            waitingQueue.push(userId);
            ws.send(JSON.stringify({ type: "waiting" }));
          }
        } else {
          waitingQueue.push(userId);
          ws.send(JSON.stringify({ type: "waiting" }));
        }
        break;
      }

      case "message": {
        const partnerId = pairs.get(userId);
        if (!partnerId) {
          ws.send(
            JSON.stringify({ type: "error", message: "Not paired yet" })
          );
          return;
        }

        const partnerWs = users.get(partnerId);
        if (partnerWs && partnerWs.readyState === 1) {
          totalMessages++;
          partnerWs.send(
            JSON.stringify({
              type: "message",
              from: userId,
              content: msg.content,
              seq: msg.seq,
              timestamp: Date.now(),
            })
          );

          // Acknowledge to sender
          ws.send(
            JSON.stringify({
              type: "ack",
              seq: msg.seq,
            })
          );
        } else {
          ws.send(
            JSON.stringify({
              type: "error",
              message: "Partner disconnected",
            })
          );
        }
        break;
      }

      default:
        ws.send(
          JSON.stringify({ type: "error", message: `Unknown type: ${msg.type}` })
        );
    }
  });

  ws.on("close", () => {
    if (userId) {
      const partnerId = pairs.get(userId);
      if (partnerId) {
        const partnerWs = users.get(partnerId);
        if (partnerWs && partnerWs.readyState === 1) {
          partnerWs.send(
            JSON.stringify({ type: "partner_disconnected" })
          );
        }
        pairs.delete(partnerId);
        pairs.delete(userId);
      }

      // Remove from waiting queue
      const qIdx = waitingQueue.indexOf(userId);
      if (qIdx !== -1) waitingQueue.splice(qIdx, 1);

      users.delete(userId);
    }
  });

  ws.on("error", (err) => {
    console.error(`WebSocket error for ${userId}:`, err.message);
  });
});

// Print stats every 2 seconds
setInterval(() => {
  console.log(
    `[Stats] Connections: ${users.size} | Pairs: ${pairs.size / 2} | Messages routed: ${totalMessages} | Waiting: ${waitingQueue.length}`
  );
}, 2000);

console.log(`WebSocket Chat Server running on ws://localhost:${PORT}`);
console.log("Protocol:");
console.log('  Register:  {"type":"register","userId":"user1"}');
console.log('  Message:   {"type":"message","content":"hello","seq":1}');
