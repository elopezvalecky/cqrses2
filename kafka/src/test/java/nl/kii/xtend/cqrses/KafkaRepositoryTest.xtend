package nl.kii.xtend.cqrses

import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.impl.CompositeFutureImpl
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import java.util.ArrayList
import java.util.Iterator
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import nl.kii.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.test.TestUtils
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.ToString
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
import io.vertx.ext.unit.junit.RepeatRule
import io.vertx.ext.unit.junit.Repeat

@RunWith(VertxUnitRunner)
class KafkaRepositoryTest {

    @ClassRule
    val public static CLUSTER = new EmbeddedSingleNodeKafkaCluster

    val protected static TOPIC = 'test.eventLog'
    val protected static STORE = 'test.aggregateStore'

    @BeforeClass
    def static void setUpTopics() {
        CLUSTER.createTopic(TOPIC)
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

    val config = new Properties => [
        put(ACKS_CONFIG, 'all')
        put(BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers)
    ]

    @Rule val public repeater = new RepeatRule

    @Repeat(5)
    @Test
    def void run(TestContext context) {
        val async = context.async
        
        // Instance A
        val instanceA = UUID.randomUUID
        val sConfigA = new Properties => [
            putAll(config)
            put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
            put(APPLICATION_ID_CONFIG, 'test')
            put(APPLICATION_SERVER_CONFIG, '''«instanceA»:10000'''.toString)
        ]
        val vertxA = ruleA.vertx
        val futureA = Future.<KafkaRepository>future
        vertxA.executeBlocking([
            val streams = createStream(sConfigA)
            TestUtils.waitForCondition([!streams.allMetadataForStore(STORE).empty], 5000, 'StreamsMetadata should be available')            
            val producer = createProducer(config)
            complete(new KafkaRepository(vertxA, instanceA, TOPIC, STORE, streams, producer))
        ], futureA.completer)
        val dataA = (1..10).map[new Demo( UUID.randomUUID, 'Text'+it)].toList
        val setupA = futureA
                        .compose [ repo|
                            val future = Future.<Void>future
                            val data = dataA.map[repo.save(it, null)]
                            CompositeFutureImpl.all(data).map[null].setHandler(future.completer)
                            future.map[repo]
                        ]

        // Instance B
        val instanceB = UUID.randomUUID
        val sConfigB = new Properties => [
            putAll(config)
            put(CLIENT_ID_CONFIG, UUID.randomUUID.toString)
            put(AUTO_OFFSET_RESET_CONFIG, 'earliest')
            put(APPLICATION_ID_CONFIG, 'test')
            put(APPLICATION_SERVER_CONFIG, '''«instanceB»:10000'''.toString)
        ]
        val vertxB = ruleB.vertx
        val futureB = Future.<KafkaRepository>future
        vertxB.executeBlocking([
            val streams = createStream(sConfigB)
            TestUtils.waitForCondition([!streams.allMetadataForStore(STORE).empty], 5000, 'StreamsMetadata should be available')            
            val producer = createProducer(config)
            complete(new KafkaRepository(vertxB, instanceB, TOPIC, STORE, streams, producer))
        ], futureB.completer)
        val dataB = (11..20).map[new Demo( UUID.randomUUID, 'Text'+it)].toList
        val setupB = futureB
                        .compose [ repo|
                            val future = Future.<Void>future
                            val data = dataB.map[repo.save(it, null)]
                            CompositeFutureImpl.all(data).map[null].setHandler(future.completer)
                            future.map[repo]
                        ]

        val futureC = Future.future
        CompositeFuture.all(setupA, setupB).setHandler(futureC.completer)
        futureC
            .compose [
                val future = Future.<Demo>future
                ruleC.vertx.executeBlocking([
                    Thread.sleep(2000)
                    setupB.compose[load(dataA.get(3).id, Demo)].setHandler(completer)
                ], future.completer)
                future            
            ]
            .setHandler [
                if (failed) context.fail(cause)
                else {
                    val expected = dataA.get(3)
                    context.assertNotNull(result)
                    context.assertEquals(expected.id, result.id)
                    context.assertEquals(expected.text, result.text)
                    async.complete
                }
            ]
    }

    def private createStream(Properties config) {
        val uuidSerde = Serdes.serdeFrom(new UUIDSerializer, new UUIDDeserializer)
        val aggregateSerde = Serdes.serdeFrom(new AggregateRootSerializer, new AggregateRootDeserializer)
        val messageSerde = Serdes.serdeFrom(new DomainEventMessageSerializer, new DomainEventMessageDeserializer)
        val eventsSerde = Serdes.serdeFrom(new JavaSerializer<ArrayList<Event>>, new JavaDeserializer<ArrayList<Event>>)
        val pairSerde = Serdes.serdeFrom(new JavaSerializer<Pair<String, UUID>>, new JavaDeserializer<Pair<String, UUID>>)

        val builder = new KStreamBuilder
        val aggregateStore = Stores.create(STORE).withKeys(uuidSerde).withValues(aggregateSerde).persistent.build
        builder.addStateStore(aggregateStore)

        val eventsPerAggregateStore = Stores.create('intermediate').withKeys(pairSerde).withValues(eventsSerde).persistent.build

        val events = builder.stream(uuidSerde, messageSerde, TOPIC)

        events
            .selectKey[k, v|v.aggregateType.name -> k]
            .groupByKey(pairSerde,messageSerde)
            .aggregate([<Event>newArrayList], [$2 => [add($1.payload)]], eventsPerAggregateStore)
            .toStream
            .transform([new AggregateRootTransformer(aggregateStore.name)],aggregateStore.name)

        new KafkaStreams(builder, config) => [
            cleanUp
            start
        ]
    }
    
    def private createProducer(Properties config) {
        new KafkaProducer(config, new UUIDSerializer, new DomainEventMessageSerializer)
    }

    @ToString
    static class Demo extends AggregateRoot {
        var String text
        new(UUID id, String text) {
           apply(new DemoAdded(id, text)) 
        }
        new(Iterator<Event> events) {
            super(events)
        }
        def void update(String text) {
            apply(new DemoTextUpdated(id, text))
        }
        def void handle(DemoAdded event) {
            this.id = event.id
            this.text = event.text
        }
        def protected void handle(DemoTextUpdated event) {
            this.text = event.text
        }
    }
    
    @Accessors(PUBLIC_GETTER)
    static class DemoAdded implements Event {
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
}