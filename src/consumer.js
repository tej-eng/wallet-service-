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
  max: 10,
})

/**
 * Prisma Adapter
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

const QUEUE_NAME = "payment.success"

async function startConsumer() {
  try {
    console.log("Connecting to RabbitMQ...")

    connection = await amqp.connect(process.env.RABBITMQ_URL)

    connection.on("error", (err) => {
      console.error("RabbitMQ connection error:", err.message)
    })

    connection.on("close", () => {
      console.warn("RabbitMQ connection closed. Reconnecting...")
      setTimeout(startConsumer, 5000)
    })

    channel = await connection.createChannel()

    /**
     * Ensure queue exists
     */
    await channel.assertQueue(QUEUE_NAME, {
      durable: true,
    })

    /**
     * Prevent parallel processing
     */
    channel.prefetch(1)

    console.log(`Waiting for messages in queue: ${QUEUE_NAME}`)

    channel.consume(
      QUEUE_NAME,
      async (msg) => {
        if (!msg) return

        let data

        try {
          data = JSON.parse(msg.content.toString())
        } catch (err) {
          console.error("Invalid JSON message")
          channel.nack(msg, false, false)
          return
        }

        console.log("Received message:", data)

        try {
          await prisma.$transaction(
            async (tx) => {

              /**
               * UPSERT WALLET
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
               * CREATE WALLET TRANSACTION
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
              timeout: 10000,
            }
          )

          console.log(
            `SUCCESS: user=${data.userId}, coins=${data.coins}, payment=${data.paymentId}`
          )

          /**
           * Acknowledge message
           */
          channel.ack(msg)

        } catch (err) {

          console.error("Processing failed:", err)

          /**
           * Reject message (do not requeue)
           */
          channel.nack(msg, false, false)
        }

      },
      {
        noAck: false,
      }
    )

  } catch (err) {

    console.error("Consumer startup failed:", err.message)

    /**
     * Retry after 5 sec
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