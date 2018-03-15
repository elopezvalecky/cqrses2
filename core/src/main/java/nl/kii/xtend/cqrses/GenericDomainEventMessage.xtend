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

import static extension java.util.UUID.fromString
import static extension java.lang.Class.forName
import static extension java.time.Instant.ofEpochMilli

@Data
@JsonSerialize(using=GenericDomainEventMessageJsonSerializer)
@JsonDeserialize(using=GenericDomainEventMessageJsonDeserializer)
class GenericDomainEventMessage<E extends Event,A extends AggregateRoot> extends GenericEventMessage<E> implements DomainEventMessage<E,A> {

    val UUID aggregateId
    val Class<A> aggregateType
    
    new(UUID id, E payload, Map<String, ?> metadata, Instant timestamp, UUID aggregateId, Class<A> aggregateType) {
        super(id, payload, metadata, timestamp)
        this.aggregateId = aggregateId
        this.aggregateType = aggregateType
    }
    
    new(DomainEventMessage<E,A> original, Map<String, ?> metadata) {
        this(original.id, original.payload, metadata, original.timestamp, original.aggregateId, original.aggregateType)
    }

    new(UUID aggregateId, Class<A> aggregateType, E payload, Map<String, ?> metadata) {
        this(UUID.randomUUID, payload, metadata, Instant.now, aggregateId, aggregateType)
    }

    new(UUID aggregateId, Class<A> aggregateType, E payload) {
        this(aggregateId, aggregateType, payload, emptyMap)
    }

    override DomainEventMessage<E,A> withMetaData(Map<String,?> metadata) {
        if (this.metadata == metadata) return this
        new GenericDomainEventMessage(this, metadata)
    }
    
    override DomainEventMessage<E,A> andMetaData(Map<String,?> metadata) {
        if (metadata.empty) return this
        val tmp = newHashMap => [
            putAll(this.metadata)
            putAll(metadata)
        ]
        new GenericDomainEventMessage(this, tmp)
    }

    override toString() '''«class.simpleName»[«aggregateType.simpleName»,«timestamp»,«payload»]'''

}

class GenericDomainEventMessageJsonSerializer extends JsonSerializer<DomainEventMessage<? extends Event, ? extends AggregateRoot>> {

    override serialize(DomainEventMessage<? extends Event,? extends AggregateRoot> value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        gen.writeStartObject
        gen.writeObjectField('id', value.id)
        gen.writeObjectField('aggregateId', value.aggregateId)
        gen.writeStringField('aggregateType', value.aggregateType.name)
        gen.writeNumberField('timestamp', value.timestamp.toEpochMilli)
        gen.writeObjectField('payload', value.payload)
        gen.writeStringField('payloadType', value.payload.class.name)
        gen.writeObjectField('metadata', value.metadata)
        gen.writeEndObject
    }

}

class GenericDomainEventMessageJsonDeserializer extends JsonDeserializer<DomainEventMessage<? extends Event,? extends AggregateRoot>> {

    val private static mapper = new ObjectMapper => [
        // Non-standard JSON but we allow C style comments in our JSON
        configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    ]

    override deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        val node = p.codec.readTree(p)

        val id = (node.get('id') as TextNode).asText.fromString
        val aggregateId = (node.get('aggregateId') as TextNode).asText.fromString
        val aggregateType = (node.get('aggregateType') as TextNode).asText.forName as Class<AggregateRoot>
        val timestamp = (node.get('timestamp') as NumericNode).asLong.ofEpochMilli
        val payloadType = (node.get('payloadType') as TextNode).asText.forName as Class<? extends Event>
        val payload = mapper.treeToValue(node.get('payload'), payloadType) 
        val metadata = mapper.treeToValue(node.get('metadata'), Map)

        new GenericDomainEventMessage(id, payload, metadata, timestamp, aggregateId, aggregateType)
    }

}