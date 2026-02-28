import amqp from "amqplib";
import { v4 as uuidv4 } from "uuid";

/*
Usage:

docker compose exec wallet-service node src/test-recharge.js <userId> <coins> <amount> <rechargePackId>

Example:

docker compose exec wallet-service node src/test-recharge.js \
2a970844-c497-4075-bccb-0f440637960a 100 50 pack_001
*/

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://rabbitmq:5672";
const QUEUE = "payment.success";

async function sendRechargeMessage() {
  try {
    const userId = process.argv[2];
    const coins = parseInt(process.argv[3] || "100");
    const amount = parseFloat(process.argv[4] || "50");
    const rechargePackId = process.argv[5] || "pack_001";

    if (!userId) {
      console.error("\n❌ ERROR: userId required\n");
      console.log("Usage:");
      console.log(
        "node src/test-recharge.js <userId> <coins> <amount> <rechargePackId>\n"
      );
      process.exit(1);
    }

    const paymentId = "pay_" + uuidv4();

    const message = {
      userId,
      coins,
      amount,
      rechargePackId,
      paymentId,
      createdAt: new Date().toISOString(),
    };

    console.log("\nConnecting to RabbitMQ:", RABBITMQ_URL);

    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE, { durable: true });

    channel.sendToQueue(
      QUEUE,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: "application/json",
      }
    );

    console.log("\n✅ Recharge message sent successfully\n");
    console.log("Queue:", QUEUE);
    console.log("Message:", message);

    await channel.close();
    await connection.close();

    process.exit(0);
  } catch (error) {
    console.error("\n❌ Failed to send recharge message\n");
    console.error(error);
    process.exit(1);
  }
}

sendRechargeMessage();
