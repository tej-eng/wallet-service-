import amqp from "amqplib";
import { v4 as uuidv4 } from "uuid";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";
const QUEUE = "payment.success";

// CLI args
const userId = process.argv[2];
const coins = parseInt(process.argv[3]) || 100;
const amount = parseInt(process.argv[4]) || 50;
const rechargePackId = process.argv[5] || "pack_001";
const totalMessages = parseInt(process.argv[6]) || 100;
const concurrency = parseInt(process.argv[7]) || 10;

if (!userId) {
  console.error("❌ Usage:");
  console.error(
    "node src/load-test-recharge.js <userId> <coins> <amount> <packId> <totalMessages> <concurrency>"
  );
  process.exit(1);
}

let sent = 0;
let success = 0;
let failed = 0;

async function createConnection() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertQueue(QUEUE, { durable: true });
  return { conn, channel };
}

async function sendRecharge(channel, index) {
  try {
    const message = {
      userId,
      coins,
      amount,
      rechargePackId,
      paymentId: `pay_${uuidv4()}`,
      createdAt: new Date().toISOString(),
    };

    const ok = channel.sendToQueue(
      QUEUE,
      Buffer.from(JSON.stringify(message)),
      { persistent: true }
    );

    if (ok) {
      success++;
    } else {
      failed++;
    }
  } catch (err) {
    failed++;
    console.error("Send error:", err.message);
  }
}

async function worker(workerId, channel) {
  while (true) {
    const current = sent++;
    if (current >= totalMessages) break;
    await sendRecharge(channel, current);
  }
}

async function main() {
  console.log("=================================");
  console.log("🚀 Recharge Load Test Started");
  console.log("=================================");

  console.log("User:", userId);
  console.log("Total Messages:", totalMessages);
  console.log("Concurrency:", concurrency);

  const start = Date.now();

  const { conn, channel } = await createConnection();

  const workers = [];

  for (let i = 0; i < concurrency; i++) {
    workers.push(worker(i, channel));
  }

  await Promise.all(workers);

  const end = Date.now();

  console.log("\n=================================");
  console.log("✅ Load Test Completed");
  console.log("=================================");

  console.log("Total Sent:", totalMessages);
  console.log("Success:", success);
  console.log("Failed:", failed);

  console.log("Time:", (end - start) / 1000, "seconds");
  console.log(
    "Throughput:",
    Math.round(totalMessages / ((end - start) / 1000)),
    "msg/sec"
  );

  await channel.close();
  await conn.close();

  process.exit(0);
}

main().catch(console.error);
