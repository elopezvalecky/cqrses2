package nl.kii.xtend.cqrses

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.shareddata.Shareable
import java.util.ArrayList
import java.util.UUID
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.state.Stores
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension io.vertx.core.logging.LoggerFactory.getLogger

@FinalFieldsConstructor
class KafkaVerticle extends AbstractVerticle {

    val logger = class.logger

    val String topic
    val String store
    
    val UUID instanceId = UUID.randomUUID
    val KStreamBuilder builder = new KStreamBuilder

    var KafkaStreams streams
    var KafkaProducer<UUID, DomainEventMessage<? extends Event, ? extends AggregateRoot>> producer

    var KafkaRepository repository

    override start(Future<Void> startFuture) throws Exception {
        vertx.executeBlocking([
            logger.info('Starting KafkaTopology')
            initializeTopology
            
            logger.info('Starting KafkaStreams with application.server set to {}', instanceId.toString)
            config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, '''«instanceId»:10000''')
            initializeStreams
            vertx.sharedData.getLocalMap('streams').put('metadata', new StreamsMetadata(streams))

            logger.info('Starting KafkaProducer')
            initializeProducer

            logger.info('Staring repository')
            initializeRepository
        ], startFuture.completer)
    }

    override stop(Future<Void> stopFuture) throws Exception {
        vertx.executeBlocking([
            logger.info('Shutting down KafkaStreams')
            streams.close
            logger.info('Shutting down KafkaProducer')
            producer.close
            complete()
        ], stopFuture.completer)
    }

    def private initializeTopology() {
        val uuidSerde = Serdes.serdeFrom(new UUIDSerializer, new UUIDDeserializer)
        val aggregateSerde = Serdes.serdeFrom(new AggregateRootSerializer, new AggregateRootDeserializer)
        val messageSerde = Serdes.serdeFrom(new DomainEventMessageSerializer, new DomainEventMessageDeserializer)
        val eventsSerde = Serdes.serdeFrom(new JavaSerializer<ArrayList<Event>>, new JavaDeserializer<ArrayList<Event>>)
        val pairSerde = Serdes.serdeFrom(new JavaSerializer<Pair<String, UUID>>, new JavaDeserializer<Pair<String, UUID>>)

        val aggregateStore = Stores.create(store).withKeys(uuidSerde).withValues(aggregateSerde).persistent.build
        builder.addStateStore(aggregateStore)

        val eventsPerAggregateStore = Stores.create('intermediate').withKeys(pairSerde).withValues(eventsSerde).persistent.build

        builder.stream(uuidSerde, messageSerde, topic)
            .selectKey[k, v|v.aggregateType.name -> k]
            .groupByKey(pairSerde,messageSerde)
            .aggregate([<Event>newArrayList], [$2 => [add($1.payload)]], eventsPerAggregateStore)
            .toStream
            .transform([new AggregateRootTransformer(aggregateStore.name)],aggregateStore.name)
    }

    
    def private initializeStreams() {
        streams = new KafkaStreams(builder, new StreamsConfig(config.map))
        streams.setStateListener [newState, oldState|
            logger.debug('State change in KafkaStreams recorded: oldstate={}, newstate={}', oldState, newState)
        ]
        streams.cleanUp
        streams.start
    }

    def private initializeProducer() {
        val config = new StreamsConfig(config.map)
        producer = new KafkaProducer(config.getProducerConfigs(UUID.randomUUID.toString), new StringSerializer, new DomainEventMessageSerializer)
    }

    def private initializeRepository() {
        repository = new KafkaRepository(vertx, instanceId, topic, store, streams, producer)
    }

    @FinalFieldsConstructor
    static class StreamsMetadata implements Shareable {

        val KafkaStreams streams

        def allMetadata() {
            streams.allMetadata()
        }
    
        def allMetadataForStore(String storeName) {
            streams.allMetadataForStore(storeName)
        }
    
        def <K> metadataForKey(String storeName, K key, Serializer<K> keySerializer) {
            streams.metadataForKey(storeName, key, keySerializer)
        }
    
        def <K> metadataForKey(String storeName, K key, StreamPartitioner<? super K, ?> partitioner) {
            streams.metadataForKey(storeName, key, partitioner)
        }
    
    }

}