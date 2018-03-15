package nl.kii.xtend.cqrses

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import java.util.UUID
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.state.Stores

import static extension io.vertx.core.logging.LoggerFactory.getLogger

abstract class KafkaVerticle extends AbstractVerticle {

    val logger = class.logger
    
    var KafkaStreams streams
    
    override start(Future<Void> startFuture) throws Exception {
        vertx.executeBlocking([
            super.start
            logger.info('Starting KafkaStreams')
    
            val stateStore = config.getString('service.state.store', stateStoreName)
            val eventTopic = config.getString('service.event.topic', eventTopicName)
            val commandTopic = config.getString('service.command.topic', commandTopicName)
            val kafkaConfig = config.getJsonObject('kafka')

            val uuidSerde = new GenericSerde<UUID>
            val aggregateSerde = new GenericSerde<AggregateRoot>
            val commandSerde = new GenericSerde<CommandMessage<? extends Command>>
            val eventSerde = new GenericSerde<DomainEventMessage<? extends Event,? extends AggregateRoot>>()

            // KafkaStreams Topology
            val builder = new KStreamBuilder

            // KafkaStreams LocalStore
            val states = Stores.create(stateStore).withKeys(uuidSerde).withValues(aggregateSerde).persistent.build
            builder.addStateStore(states)        
            
            // Stream processing for commands
            builder
                .stream(uuidSerde, commandSerde, commandTopic)
                .transform([commandHandler], stateStore)
                .filter[k,v|v !== null]
                .mapValues[aggregate|aggregate.unsaved.map[new GenericDomainEventMessage(aggregate.id, aggregate.class, it)]]
                .flatMapValues[toIterable as Iterable<? extends DomainEventMessage<? extends Event,? extends AggregateRoot>>]
                .to(uuidSerde, eventSerde, eventTopic)

            logger.info('Starting up KafkaStreams')
            streams = new KafkaStreams(builder, new StreamsConfig(kafkaConfig.map))
            streams.setStateListener [newState, oldState|
                logger.debug('State change in KafkaStreams recorded: oldstate={}, newstate={}', oldState, newState)
            ]
            streams.start
            complete()
        ], startFuture.completer)
    }
    
    override stop(Future<Void> stopFuture) throws Exception {
        vertx.executeBlocking([
            logger.info('Stopping KafkaStreams')
        ], stopFuture.completer)
    }

    def String getStateStoreName()
    def String getEventTopicName()
    def String getCommandTopicName()
    def KafkaCommandHandler getCommandHandler()
    
}