package nl.kii.xtend.cqrses

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.TextNode
import java.io.IOException
import java.time.Instant
import java.util.Map
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Data

import static extension java.lang.Class.forName
import static extension java.time.Instant.ofEpochMilli
import static extension java.util.UUID.fromString

@Data
@JsonSerialize(using=GenericEventMessageJsonSerializer)
@JsonDeserialize(using=GenericEventMessageJsonDeserializer)
class GenericEventMessage<E extends Event> extends GenericMessage<E> implements EventMessage<E> {
    
    val Instant timestamp
    
    def static <E extends Event> EventMessage<E> asEventMessage(Object event) {
        if (EventMessage.isInstance(event)) {
            return event as EventMessage<E>
        } else if (event instanceof Message<?>) {
            val message = event as Message<E>
            return new GenericEventMessage<E>(message.payload, message.metadata)
        }
        return new GenericEventMessage<E>(event as E);
    }
    
    new(UUID id, E payload, Map<String,?> metadata, Instant timestamp) {
        super(id, payload, metadata)
        this.timestamp = timestamp
    }

    new(EventMessage<E> original, Map<String, ?> metadata) {
        this(original.id, original.payload, metadata, original.timestamp)
    }

    new(E payload, Map<String, ?> metadata) {
        this(UUID.randomUUID, payload, metadata, Instant.now)
    }

    new(E payload) {
        this(payload, emptyMap)
    }
    
    override EventMessage<E> withMetaData(Map<String,?> metadata) {
        if (this.metadata == metadata) return this
        new GenericEventMessage<E>(this, metadata)
    }
    
    override EventMessage<E> andMetaData(Map<String,?> metadata) {
        if (metadata.empty) return this
        val tmp = newHashMap => [
            putAll(this.metadata)
            putAll(metadata)
        ]
        new GenericEventMessage<E>(this, tmp)
    }

    override toString() '''«class.simpleName»[«timestamp»,«payload»]'''

}

class GenericEventMessageJsonSerializer extends JsonSerializer<EventMessage<? extends Event>> {

    override serialize(EventMessage<? extends Event> value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        gen.writeStartObject
        gen.writeObjectField('id', value.id)
        gen.writeNumberField('timestamp', value.timestamp.toEpochMilli)
        gen.writeObjectField('payload', value.payload)
        gen.writeStringField('payloadType', value.payload.class.name)
        gen.writeObjectField('metadata', value.metadata)
        gen.writeEndObject
    }

}

class GenericEventMessageJsonDeserializer extends JsonDeserializer<EventMessage<? extends Event>> {

    val private static mapper = new ObjectMapper => [
        // Non-standard JSON but we allow C style comments in our JSON
        configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    ]

    override deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        val node = p.codec.readTree(p)

        val id = (node.get('id') as TextNode).asText.fromString
        val timestamp = (node.get('timestamp') as NumericNode).asLong.ofEpochMilli
        val payloadType = (node.get('payloadType') as TextNode).asText.forName as Class<? extends Event>
        val payload = mapper.treeToValue(node.get('payload'), payloadType) 
        val metadata = mapper.treeToValue(node.get('metadata'), Map)

        new GenericEventMessage(id, payload, metadata, timestamp)
    }

}