// src/consumer.js

import "dotenv/config"
import amqp from "amqplib"

import pkg from "@prisma/client"
const { PrismaClient } = pkg

import pg from "pg"
const { Pool } = pg

import { PrismaPg } from "@prisma/adapter-pg"


/**
 * PostgreSQL Pool
 */
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10, // limit connections
})


/**
 * Prisma Adapter (Prisma v7)
 */
const adapter = new PrismaPg(pool)


/**
 * Prisma Client
 */
const prisma = new PrismaClient({
  adapter,
  log: ["error", "warn"],
})


let connection
let channel


async function startConsumer() {
  try {

    console.log("Connecting to RabbitMQ...")

    connection = await amqp.connect(process.env.RABBITMQ_URL)

    channel = await connection.createChannel()

    const queue = "payment.success"

    /**
     * VERY IMPORTANT
     * Prevent multiple parallel transactions
     */
    channel.prefetch(1)

    console.log(`Waiting for messages in queue: ${queue}`)


    channel.consume(
      queue,
      async (msg) => {

        if (!msg) return

        const data = JSON.parse(msg.content.toString())

        console.log("Received message:", data)

        try {

          await prisma.$transaction(
            async (tx) => {

              /**
               * 1. UPSERT WALLET
               */
              const wallet = await tx.userWallet.upsert({
                where: {
                  userId: data.userId,
                },
                update: {
                  balanceCoins: {
                    increment: data.coins,
                  },
                },
                create: {
                  userId: data.userId,
                  balanceCoins: data.coins,
                  lockedCoins: 0,
                },
              })


              /**
               * 2. CREATE TRANSACTION
               */
              await tx.walletTransaction.create({
                data: {
                  userWalletId: wallet.id,
                  rechargePackId: data.rechargePackId,
                  type: "CREDIT",
                  coins: data.coins,
                  amount: data.amount,
                  description: "Recharge successful",
                },
              })

            },
            {
              timeout: 10000, // prevent hanging tx
            }
          )


          console.log(
            `SUCCESS: user=${data.userId}, coins=${data.coins}, payment=${data.paymentId}`
          )

          channel.ack(msg)

        } catch (err) {

          console.error("Processing failed:", err.message)

          /**
           * reject message
           */
          channel.nack(msg, false, false)
        }

      },
      {
        noAck: false,
      }
    )

  } catch (err) {

    console.error("Consumer startup failed:", err)

    /**
     * Retry connection
     */
    setTimeout(startConsumer, 5000)

  }
}


/**
 * Graceful shutdown
 */
async function shutdown() {

  console.log("Shutting down consumer...")

  try {

    if (channel) await channel.close()

    if (connection) await connection.close()

    await prisma.$disconnect()

    await pool.end()

  } catch (err) {

    console.error("Shutdown error:", err)

  }

  process.exit(0)
}


process.on("SIGINT", shutdown)
process.on("SIGTERM", shutdown)


/**
 * Start Consumer
 */
startConsumer()