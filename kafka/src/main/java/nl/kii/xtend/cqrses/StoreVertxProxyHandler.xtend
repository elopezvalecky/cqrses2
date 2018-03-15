package nl.kii.xtend.cqrses

import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.serviceproxy.ProxyHandler
import io.vertx.serviceproxy.ServiceException
import io.vertx.serviceproxy.ServiceExceptionMessageCodec

import static extension java.util.UUID.fromString

class StoreVertxProxyHandler extends ProxyHandler {
    
    val static DEFAULT_CONNECTION_TIMEOUT = 5 * 60 // 5 minutes
    
    val Vertx vertx
    val Store service
    val long timerID
    var long lastAccessed
    val long timeoutSeconds

    new(Vertx vertx, Store service) {
        this(vertx, service, DEFAULT_CONNECTION_TIMEOUT)
    }

    new(Vertx vertx, Store service, long timeoutInSecond) {
        this(vertx, service, true, timeoutInSecond)
    }

    new(Vertx vertx, Store service, boolean topLevel_finalParam_, long timeoutSeconds) {
        var topLevel = topLevel_finalParam_
        this.vertx = vertx
        this.service = service
        this.timeoutSeconds = timeoutSeconds
        try {
            this.vertx.eventBus.registerDefaultCodec(ServiceException, new ServiceExceptionMessageCodec)
        } catch (IllegalStateException ex) {
        }

        if (timeoutSeconds !== -1 && !topLevel) {
            var long period = timeoutSeconds * 1000 / 2
            if (period > 10000) {
                period = 10000
            }
            this.timerID = vertx.setPeriodic(period)[checkTimedOut(it)]
        } else {
            this.timerID = -1
        }
        accessed
    }

    override MessageConsumer<JsonObject> registerHandler(String address) {
        var MessageConsumer<JsonObject> consumer = vertx.eventBus.<JsonObject>consumer(address).handler(this)
        this.setConsumer(consumer)
        return consumer
    }

    def private void checkTimedOut(long id) {
        var long now = System.nanoTime
        if (now - lastAccessed > timeoutSeconds * 1000000000) {
            close
        }
    }

    override void close() {
        if (timerID !== -1) {
            vertx.cancelTimer(timerID)
        }
        super.close
    }

    def private void accessed() {
        this.lastAccessed = System.nanoTime
    }

    override void handle(io.vertx.core.eventbus.Message<JsonObject> msg) {
        try {
            var JsonObject json = msg.body
            var String action = msg.headers.get('action')
            if (action === null) {
                throw new IllegalStateException('action not specified')
            }
            accessed()

            switch (action) {
                case 'load': {
                    val id = json.getString('id').fromString
                    val type = Class.forName(json.getString(('type'))) as Class<? extends AggregateRoot>
                    service.load(id, type) [
                        if (failed) {
                            val ex = if (cause instanceof ServiceException) {
                                cause
                            } else {
                                new ServiceException(-1, cause.message)
                            }
                            msg.reply(ex)
                        } else {
                            msg.reply(result)
                        }
                    ]
                }
                default: {
                    throw new IllegalStateException('''Invalid action: «action»''')
                }
            }
        } catch (Throwable t) {
            msg.reply(new ServiceException(500, t.getMessage()))
            throw t
        }

    }

}
