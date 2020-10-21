package com.grmcosta

import io.vertx.core.AbstractVerticle
import io.vertx.core.Handler
import io.vertx.ext.amqp.AmqpClient
import io.vertx.ext.amqp.AmqpClientOptions
import io.vertx.ext.amqp.AmqpMessage
import org.slf4j.LoggerFactory

class ProducerVerticle : AbstractVerticle() {

    private val LOGGER = LoggerFactory.getLogger(ProducerVerticle::class.java)

    private var client : AmqpClient? = null

    @Throws(Exception::class)
    override fun start() {
        val options = AmqpClientOptions()
                .setHost("Localhost")
                .setUsername("guest")
                .setPassword("guest")
                .setPort(5672)

        client = AmqpClient.create(vertx, options)

        client?.connect { ar ->
            if (ar.failed())
                LOGGER.info("Unable to connect to the broker")
            else{
                LOGGER.info("Connection succeeded")
                val connection = ar.result()
                connection.createSender("my-queue", Handler { done ->
                    if(done.failed())
                        LOGGER.info("Unable to create a sender")
                    else{
                        LOGGER.info("Sender created")
                        val sender = done.result()
                        vertx.setPeriodic(5000, Handler { x ->
                            LOGGER.info("Sending Hello to consumer")
                            sender.send(AmqpMessage.create().withBody("Hello from Producer").build())
                        })
                    }
                })
            }
        }
        super.start()
    }

    @Throws(Exception::class)
    override fun stop() {
        client?.close { close ->
            if(close.succeeded()){
                LOGGER.info("AmqpClient closed successfully")
            }
        }
        super.stop()
    }
}