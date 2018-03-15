package nl.kii.xtend.cqrses

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import io.vertx.serviceproxy.ProxyHelper
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.UUID
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.clients.producer.ProducerRecord

import static extension io.vertx.core.logging.LoggerFactory.getLogger
import static extension java.util.UUID.fromString
import io.vertx.core.CompositeFuture
import java.util.Map

class KafkaRepository implements AsyncRepository {

    val static STORE_ADDRESS_PREFIX = 'kafka:store#'

    val logger = class.logger

    val Vertx vertx

    val UUID instanceId

    val String topic
    val String store

    val KafkaStreams streams
    val KafkaProducer<UUID, DomainEventMessage<? extends Event,? extends AggregateRoot>> producer
    
    val MessageConsumer<JsonObject> proxy

    new(Vertx vertx, UUID instanceId, String topic, String store, KafkaStreams streams, KafkaProducer<UUID, DomainEventMessage<? extends Event,? extends AggregateRoot>> producer) {
        this.instanceId = instanceId
        this.topic = topic
        this.store = store
        this.streams = streams
        this.producer = producer
        this.vertx = vertx
        this.proxy = ProxyHelper.registerService(Store, vertx, new Store {
            override <AR extends AggregateRoot> load(UUID id, Class<AR> type, Handler<AsyncResult<Buffer>> result) {
                localLoad(id, type).serialize.setHandler(result)
            }
        }, STORE_ADDRESS_PREFIX + instanceId.toString)
    }
    
    override <AR extends AggregateRoot> load(UUID aggregateId, Class<AR> aggregateType) {
        val future = Future.future
        vertx.executeBlocking([
            val metadata = this.streams.metadataForKey(this.store, aggregateId, new UUIDSerializer)
            if (metadata === null) {
                localLoad(aggregateId, aggregateType).setHandler(completer)
            } else {
                Future.succeededFuture(metadata.host.fromString)
                    .compose[instanceId|
                        if (this.instanceId.equals(instanceId)) {
                            localLoad(aggregateId, aggregateType)
                        } else {
                            remoteLoad(instanceId, aggregateId, aggregateType)
                        }
                    ]
                    .setHandler(completer)
            }
            
        ], future.completer)
        future
    }
    
    override <AR extends AggregateRoot> save(AR aggregate, Map<String, ?> metadata) {
        val futures = newArrayList
        aggregate
            .unsaved
            .map [new GenericDomainEventMessage(aggregate.id, aggregate.class, it, metadata?:emptyMap)]
            .forEach [
                val recordFuture = Future.<Void>future
                futures.add(recordFuture)
                val ProducerRecord<UUID, DomainEventMessage<? extends Event,? extends AggregateRoot>> record = new ProducerRecord(this.topic, aggregateId, it)
                this.producer.send(record) [ rmd, ex |
                    if (ex !== null) {
                        recordFuture.fail(ex)
                        logger.error('Send failed for record {}', record, ex)
                    } else {
                        recordFuture.complete()
                    }
            ]
        ]
        this.producer.flush
        Future.<Void>future => [ f |
            if (futures.empty) {
                f.complete()
            } else {
                CompositeFuture.all(futures).setHandler [
                    if (failed) {
                        f.fail(cause)
                    } else {
                        f.complete()
                    }
                ]
            }
        ]
    }

    def private <AR extends AggregateRoot> localLoad(UUID aggregateId, Class<AR> aggregateClass) {
        val future = Future.future
        this.vertx.executeBlocking([
            val store = this.streams.store(this.store, QueryableStoreTypes.<UUID, AR>keyValueStore)
            try {
                val aggregate = store.get(aggregateId)
                complete(aggregateClass.cast(aggregate))
            } catch (Exception e) {
                fail(e)
            }
        ],future.completer)
        future
    }
    
    def private <AR extends AggregateRoot> remoteLoad(UUID instanceId, UUID aggregateId, Class<AR> aggregateType) {
        val future = Future.future
        val proxy = ProxyHelper.createProxy(Store, this.vertx, STORE_ADDRESS_PREFIX + instanceId.toString)
        proxy.load(aggregateId, aggregateType, future.completer)
        future.deserialize(aggregateType)
    }

    def private <AR extends AggregateRoot> serialize(Future<AR> future) {
        future.map [
            val baos = new ByteArrayOutputStream
            val out = new ObjectOutputStream(baos)
            out.writeObject(it)
            val data = baos.toByteArray => [
                out.close
                baos.close
            ]
            Buffer.buffer(data)
        ]
    }
    
    def private <AR extends AggregateRoot> deserialize(Future<Buffer> future, Class<AR> clazz) {
        future.map [
            if(it === null) return null
            val bais = new ByteArrayInputStream(bytes)
            val in = new ObjectInputStream(bais)
            clazz.cast(in.readObject) => [
                in.close
                bais.close
            ]
        ]        
    }    

}