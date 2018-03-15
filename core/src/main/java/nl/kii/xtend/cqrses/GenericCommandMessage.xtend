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
import java.util.Map
import java.util.UUID
import org.eclipse.xtend.lib.annotations.Data
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

import static extension java.lang.Class.forName
import static extension java.util.UUID.fromString

@Data
@FinalFieldsConstructor
@JsonSerialize(using=GenericCommandMessageJsonSerializer)
@JsonDeserialize(using=GenericCommandMessageJsonDeserializer)
class GenericCommandMessage<C extends Command> implements CommandMessage<C> {

    val UUID id
    val C payload
    val Map<String, ?> metadata
    val String commandName
    
    new(CommandMessage<C> original, Map<String, ?> metadata) {
        this(original.id, original.payload, metadata, original.commandName)
    }

    override withMetaData(Map<String, ?> metadata) {
        if (metadata.empty) return this
        val tmp = newHashMap => [
            putAll(this.metadata)
            putAll(metadata)
        ]
        new GenericCommandMessage<C>(this, tmp)
    }
    
    override andMetaData(Map<String, ?> metadata) {
        if (this.metadata == metadata) return this
        new GenericCommandMessage<C>(this, metadata)
    }
    
    override getPayloadType() {
        payload.class as Class<C>
    }

}

class GenericCommandMessageJsonSerializer extends JsonSerializer<CommandMessage<? extends Command>> {

    override serialize(CommandMessage<? extends Command> value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        gen.writeStartObject
        gen.writeObjectField('id', value.id)
        gen.writeStringField('commandName', value.commandName)
        gen.writeObjectField('payload', value.payload)
        gen.writeStringField('payloadType', value.payload.class.name)
        gen.writeObjectField('metadata', value.metadata)
        gen.writeEndObject
    }

}

class GenericCommandMessageJsonDeserializer extends JsonDeserializer<CommandMessage<? extends Command>> {

    val private static mapper = new ObjectMapper => [
        // Non-standard JSON but we allow C style comments in our JSON
        configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    ]

    override deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        val node = p.codec.readTree(p)

        val id = (node.get('id') as TextNode).asText.fromString
        val commandName = (node.get('commandName') as TextNode).asText
        val payloadType = (node.get('payloadType') as TextNode).asText.forName as Class<? extends Command>
        val payload = mapper.treeToValue(node.get('payload'), payloadType) 
        val metadata = mapper.treeToValue(node.get('metadata'), Map)

        new GenericCommandMessage(id, payload, metadata, commandName)
    }

}