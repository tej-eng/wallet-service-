// src/consumer.js

import 'dotenv/config'
import amqp from 'amqplib'

import pkg from '@prisma/client'
const { PrismaClient } = pkg

import pg from 'pg'
const { Pool } = pg

import { PrismaPg } from '@prisma/adapter-pg'


/**
 * PostgreSQL connection pool
 * IMPORTANT:
 * In docker-compose use service name "postgres"
 * NOT localhost
 */
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
})


/**
 * Prisma adapter (required for Prisma v7)
 */
const adapter = new PrismaPg(pool)


/**
 * Prisma client instance
 */
const prisma = new PrismaClient({
  adapter,
  log: ['error', 'warn'],
})


/**
 * RabbitMQ consumer startup
 */
async function startConsumer() {
  try {
    console.log("Connecting to RabbitMQ...")

    const conn = await amqp.connect(process.env.RABBITMQ_URL)

    const channel = await conn.createChannel()

    const queue = "payment.success"

    await channel.assertQueue(queue, { durable: true })

    console.log(`Waiting for messages in queue: ${queue}`)


    channel.consume(
      queue,
      async (msg) => {

        if (!msg) return

        try {

          const data = JSON.parse(msg.content.toString())

          console.log("Received message:", data)


          await prisma.$transaction(async (tx) => {

            /**
             * 1. Upsert wallet
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
             * 2. Insert transaction
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

          })


          console.log(
            `SUCCESS: user=${data.userId}, coins=${data.coins}, payment=${data.paymentId}`
          )


          /**
           * ACK message
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

    console.error("Consumer startup failed:", err)

    process.exit(1)

  }
}


/**
 * Graceful shutdown
 */
process.on("SIGINT", async () => {

  console.log("Shutting down consumer...")

  await prisma.$disconnect()

  await pool.end()

  process.exit(0)

})


process.on("SIGTERM", async () => {

  console.log("Shutting down consumer...")

  await prisma.$disconnect()

  await pool.end()

  process.exit(0)

})


/**
 * Start consumer
 */
startConsumer()