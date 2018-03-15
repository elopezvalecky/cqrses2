package nl.kii.xtend.cqrses

import com.fasterxml.jackson.databind.SerializationFeature
import io.vertx.core.CompositeFuture
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.Iterator
import java.util.Random
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import nl.kii.xtend.cqrses.KafkaVerticle.StreamsMetadata
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.test.TestUtils
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.ToString
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import static org.apache.kafka.clients.producer.ProducerConfig.*
import static org.apache.kafka.streams.StreamsConfig.*
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import static org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG

@RunWith(VertxUnitRunner)
class KafkaVerticleTest {
    
    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster

    val protected static TOPIC = 'test.eventLog'
    val protected static STORE = 'test.aggregateStore'

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(TOPIC)
    }

    @BeforeClass
    def static void setUpObjectMapper() {
        Json.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        Json.prettyMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    }

    @BeforeClass
    def static void setUpJVM() {
        System.setProperty('vertx.logger-delegate-factory-class-name','io.vertx.core.logging.SLF4JLogDelegateFactory')
        System.setProperty('hazelcast.logging.type', 'slf4j')
    }

    val protected Supplier<Vertx> supplier = [
        val latch = new CountDownLatch(1)
        val vertx = new AtomicReference<Vertx>
        Vertx.clusteredVertx(new VertxOptions().setClusterHost('127.0.0.1')) [
            if(failed) throw new RuntimeException('Unable to create clustered Vertx')
            vertx.set(result)
            latch.countDown
        ]
        try {
            latch.await
        } catch (InterruptedException e) {
            throw new RuntimeException(e)
        }
        vertx.get
    ]

    @Rule val public ruleA = new RunTestOnContext(supplier)
    @Rule val public ruleB = new RunTestOnContext(supplier)
    @Rule val public ruleC = new RunTestOnContext(supplier)

    val config = new JsonObject => [
        put(ACKS_CONFIG, 'all')
        put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
        put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
        put(APPLICATION_ID_CONFIG, 'test')
    ]
    
    var UUID selected
    
    @Before
    def void setUp(TestContext context) {
        val producer = new KafkaProducer(config.map, new UUIDSerializer, new DomainEventMessageSerializer)

        val data = (1..50).map[new Demo(UUID.randomUUID, 'Text'+it)].toList
        data.forEach[aggregate|
            aggregate.unsaved
                .map[new GenericDomainEventMessage(aggregate.id, aggregate.class, it, emptyMap)]
                .forEach[producer.send(new ProducerRecord(TOPIC, aggregate.id, it)) ]
        ]
        
        val rnd = new Random;
        (0..100).forEach[
            val demo = data.get(rnd.nextInt(50))
            val opt = rnd.nextInt(3)
            switch opt {
                case 0:{demo.update(RandomStringUtils.randomAlphabetic(10))}
                case 1:{demo.delete}
                default:{}
            }
            demo.unsaved
                .map[new GenericDomainEventMessage(demo.id, demo.class, it, emptyMap)]
                .forEach[producer.send(new ProducerRecord(TOPIC, demo.id, it)) ]
            if (it === 50) selected = demo.id
        ]
    }

    @Test
    def void run(TestContext context) {
        val async = context.async
        
        val futureA = Future.<Void>future
        ruleA.vertx.executeBlocking([
            val config = config.copy.put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            ruleA.vertx.deployVerticle(new KafkaVerticle(TOPIC, STORE), new DeploymentOptions().setConfig(config))
            TestUtils.waitForCondition([
                val StreamsMetadata metadata = ruleA.vertx.sharedData.getLocalMap('streams').get('metadata')
                metadata !== null && !metadata.allMetadataForStore(STORE).empty
            ], 5000, 'StreamsMetadata should be available')            
            complete()            
        ], futureA.completer)
        
        val futureB = Future.<Void>future
        ruleB.vertx.executeBlocking([
            val config = config.copy.put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            ruleB.vertx.deployVerticle(new KafkaVerticle(TOPIC, STORE), new DeploymentOptions().setConfig(config))
            TestUtils.waitForCondition([
                val StreamsMetadata metadata = ruleB.vertx.sharedData.getLocalMap('streams').get('metadata')
                metadata !== null && !metadata.allMetadataForStore(STORE).empty
            ], 5000, 'StreamsMetadata should be available')            
            complete()            
        ], futureB.completer)

        CompositeFuture.all(futureA, futureB)
            .setHandler [
                if (failed) context.fail(cause)
                else {
                    Thread.sleep(2000)
                    async.complete
                }
            ]
    }

    @ToString
    static class Demo extends AggregateRoot {
        var String text
        var boolean deleted
        new(UUID id, String text) {
           apply(new DemoCreated(id, text)) 
        }
        new(Iterator<Event> events) {
            super(events)
        }
        def void update(String text) {
            apply(new DemoTextUpdated(this.id, text))
        }
        def void delete() {
            if (!deleted) apply(new DemoDeleted(this.id))
        }
        def void handle(DemoCreated event) {
            this.id = event.id
            this.text = event.text
            this.deleted = false
        }
        def protected void handle(DemoTextUpdated event) {
            this.text = event.text
        }
        def private void handle(DemoDeleted event) {
            this.deleted = false
        }
    }
    
    @Accessors(PUBLIC_GETTER)
    static class DemoCreated implements Event {
        var UUID id
        var String text
        private new() {}
        new(UUID id, String text) {
            this.id = id
            this.text = text
        }
    }

    @Accessors(PUBLIC_GETTER)
    static class DemoTextUpdated implements Event {
        var UUID id
        var String text
        private new() {}
        new(UUID id, String text) {
            this.id = id
            this.text = text
        }
    }

    @Accessors(PUBLIC_GETTER)
    static class DemoDeleted implements Event {
        var UUID id
        private new() {}
        new(UUID id) {
            this.id = id
        }
    }
}