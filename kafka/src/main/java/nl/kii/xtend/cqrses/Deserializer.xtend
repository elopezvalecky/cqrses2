package nl.kii.xtend.cqrses

import io.vertx.core.json.Json
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.Map
import java.util.UUID
import org.apache.kafka.common.serialization.Deserializer

class DomainEventMessageDeserializer implements Deserializer<DomainEventMessage<? extends Event,? extends AggregateRoot>> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override deserialize(String topic, byte[] data) {
        Json.mapper.readValue(data, GenericDomainEventMessage)
    }

}

class AggregateRootDeserializer implements Deserializer<AggregateRoot> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override deserialize(String topic, byte[] data) {
        val bais = new ByteArrayInputStream(data)
        val in = new ObjectInputStream(bais)
        in.readObject as AggregateRoot => [
            in.close
            bais.close
        ]
    }

}

class JavaDeserializer<T extends Serializable> implements Deserializer<T> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override deserialize(String topic, byte[] data) {
        val bais = new ByteArrayInputStream(data)
        val in = new ObjectInputStream(bais)
        in.readObject as T => [
            in.close
            bais.close
        ]
    }

}

class UUIDDeserializer implements Deserializer<UUID> {
    
    override close() {}
    
    override configure(Map<String, ?> config, boolean isKey) {}
    
    override deserialize(String topic, byte[] data) {
        if (data === null) null
        else {
            val bb = ByteBuffer.wrap(data)
            val firstLong = bb.getLong
            val secondLong = bb.getLong
            return new UUID(firstLong, secondLong)            
        }
    }
    
}