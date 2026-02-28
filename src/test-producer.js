// test-producer.js
import amqp from "amqplib";
import { randomUUID } from "crypto";

/**
 * Usage:
 * docker compose exec wallet-service node src/test-producer.js <USER_ID>
 *
 * OR
 * USER_ID=xxxx docker compose exec wallet-service node src/test-producer.js
 */

const USER_ID =
  process.argv[2] ||
  process.env.USER_ID ||
  "REPLACE_WITH_REAL_USER_ID";

const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";

const QUEUE = "payment.success";

async function sendTestMessage() {
  let conn;
  let ch;

  try {
    if (!USER_ID || USER_ID === "REPLACE_WITH_REAL_USER_ID") {
      console.error(
        "❌ ERROR: Please provide valid userId\n\nExample:\n docker compose exec wallet-service node src/test-producer.js <USER_ID>"
      );
      process.exit(1);
    }

    console.log("Connecting to RabbitMQ:", RABBITMQ_URL);

    conn = await amqp.connect(RABBITMQ_URL);

    ch = await conn.createChannel();

    await ch.assertQueue(QUEUE, { durable: true });

    const message = {
      userId: USER_ID,
      coins: 100,
      amount: 50,
      rechargePackId: "pack_001",
      paymentId: "pay_" + randomUUID(), // unique id every time
      createdAt: new Date().toISOString(),
    };

    ch.sendToQueue(
      QUEUE,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: "application/json",
      }
    );

    console.log("\n✅ Message sent successfully");
    console.log("Queue:", QUEUE);
    console.log("Message:", message);
  } catch (err) {
    console.error("❌ Error sending message:", err);
  } finally {
    try {
      if (ch) await ch.close();
      if (conn) await conn.close();
    } catch {}

    process.exit(0);
  }
}

sendTestMessage();