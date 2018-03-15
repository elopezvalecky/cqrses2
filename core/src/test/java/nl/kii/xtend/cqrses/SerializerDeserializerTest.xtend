package nl.kii.xtend.cqrses

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.Instant
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Accessors
import org.junit.Assert
import org.junit.Test
import org.eclipse.xtend.lib.annotations.ToString
import org.eclipse.xtend.lib.annotations.EqualsHashCode

class SerializerDeserializerTest {

    val private static mapper = new ObjectMapper

    @Test
    def void genericMessage() {
        val msg = new GenericMessage(UUID.randomUUID, 'test', emptyMap)

        val json = mapper.writeValueAsString(msg)
        val result = mapper.readValue(json, GenericMessage)

        Assert.assertEquals(msg, result)
    }

    @Test
    def void genericEventMessage() {
        val msg = new GenericEventMessage(UUID.randomUUID, new DemoHappened(UUID.randomUUID), emptyMap, Instant.now)

        val json = mapper.writeValueAsString(msg)
        val result = mapper.readValue(json, GenericEventMessage)

        Assert.assertEquals(msg.id, result.id)
        Assert.assertEquals(msg.payloadType, result.payloadType)
        Assert.assertEquals(msg.timestamp, result.timestamp)
        Assert.assertEquals(msg.payload, result.payload)
        Assert.assertEquals(msg.metadata, result.metadata)
        Assert.assertEquals(msg, result)
    }

    @Test
    def void genericDomainEventMessage() {
        val aggregate = new Demo(UUID.randomUUID)
        val messages = aggregate.unsaved.map[new GenericDomainEventMessage(UUID.randomUUID, it, emptyMap, Instant.now, aggregate.id, aggregate.class)]
        
        messages.forEach[ msg|
            val json = mapper.writeValueAsString(msg)
            val result = mapper.readValue(json, GenericDomainEventMessage)
    
            Assert.assertEquals(msg.id, result.id)
            Assert.assertEquals(msg.payloadType, result.payloadType)
            Assert.assertEquals(msg.timestamp, result.timestamp)
            Assert.assertEquals(msg.payload, result.payload)
            Assert.assertEquals(msg.metadata, result.metadata)
            Assert.assertEquals(aggregate.id, result.aggregateId)
            Assert.assertEquals(aggregate.class, result.aggregateType)
            Assert.assertEquals(msg, result)
        ]
    }

    @ToString
    @EqualsHashCode
    @Accessors(PUBLIC_GETTER)
    static class DemoHappened implements Event {
        var UUID id
        private new() {}
        new(UUID id) {
            this.id = id
        }
    }

    @ToString
    @EqualsHashCode
    @Accessors(PUBLIC_GETTER)
    static class Demo extends AggregateRoot {
        var UUID id
        new(UUID id) {
            apply(new DemoHappened(id))
        }
        def void handle(DemoHappened event) {
            this.id = event.id
        }
    }

}
