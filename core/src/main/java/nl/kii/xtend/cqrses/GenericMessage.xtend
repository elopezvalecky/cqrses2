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
import com.fasterxml.jackson.databind.node.TextNode
import java.io.IOException
import java.util.HashMap
import java.util.Map
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Data

import static extension java.lang.Class.forName
import static extension java.util.UUID.fromString

@Data
@JsonSerialize(using=GenericMessageJsonSerializer)
@JsonDeserialize(using=GenericMessageJsonDeserializer)
class GenericMessage<T> implements Message<T> {
    
    val UUID id
    val T payload
    val Map<String,?> metadata
    
    new(UUID id, T payload, Map<String,?> metadata) {
        this.id = id
        this.payload = payload
        this.metadata = metadata
    }
    
    new(Message<T> original, Map<String,?> metadata) {
        this(original.id, original.payload, new HashMap<String, Object>(metadata))
    }
    
    new(T payload, Map<String,?> metadata) {
        this(UUID.randomUUID, payload, metadata)
    }
    
    new(T payload) {
        this(payload, emptyMap)
    }
    
    override withMetaData(Map<String,?> metadata) {
        if (this.metadata == metadata) return this
        new GenericMessage<T>(this, metadata)
    }
    
    override andMetaData(Map<String,?> metadata) {
        if (metadata.empty) return this
        val tmp = newHashMap => [
            putAll(this.metadata)
            putAll(metadata)
        ]
        new GenericMessage<T>(this, tmp)
    }
    
    override toString() '''«class.simpleName»[«payload»]'''
    
    override getPayloadType() {
        this.payload.class as Class<T>
    }

}

class GenericMessageJsonSerializer extends JsonSerializer<Message<?>> {

    override serialize(Message<?> value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        gen.writeStartObject
        gen.writeObjectField('id', value.id)
        gen.writeObjectField('payload', value.payload)
        gen.writeStringField('payloadType', value.payload.class.name)
        gen.writeObjectField('metadata', value.metadata)
        gen.writeEndObject
    }

}

class GenericMessageJsonDeserializer extends JsonDeserializer<Message<?>> {

    val private static mapper = new ObjectMapper => [
        // Non-standard JSON but we allow C style comments in our JSON
        configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    ]

    override deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        val node = p.codec.readTree(p)

        val id = (node.get('id') as TextNode).asText.fromString
        val payloadType = (node.get('payloadType') as TextNode).asText.forName
        val payload = mapper.treeToValue(node.get('payload'), payloadType) 
        val metadata = mapper.treeToValue(node.get('metadata'), Map)

        new GenericMessage(id, payload, metadata)
    }

}