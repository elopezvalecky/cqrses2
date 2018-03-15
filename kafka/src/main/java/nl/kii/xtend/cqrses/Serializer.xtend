package nl.kii.xtend.cqrses

import io.vertx.core.json.Json
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.Map
import java.util.UUID
import org.apache.kafka.common.serialization.Serializer

class DomainEventMessageSerializer implements Serializer<DomainEventMessage<? extends Event,? extends AggregateRoot>> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override serialize(String topic, DomainEventMessage<? extends Event,? extends AggregateRoot> data) {
        Json.mapper.writeValueAsBytes(data)
    }

}

class AggregateRootSerializer implements Serializer<AggregateRoot> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override serialize(String topic, AggregateRoot data) {
        val baos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(baos)
        out.writeObject(data)
        baos.toByteArray => [
            out.close
            baos.close
        ]
    }
}

class JavaSerializer<T extends Serializable> implements Serializer<T> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override serialize(String topic, T data) {
        val baos = new ByteArrayOutputStream
        val out = new ObjectOutputStream(baos)
        out.writeObject(data)
        baos.toByteArray => [
            out.close
            baos.close
        ]
    }
}

class UUIDSerializer implements Serializer<UUID> {
    
    override close() {}
    
    override configure(Map<String, ?> configs, boolean isKey) {}
    
    override serialize(String topic, UUID data) {
        if (data === null) null
        else {
            val bb = ByteBuffer.wrap(newByteArrayOfSize(16))
            bb.putLong(data.mostSignificantBits)
            bb.putLong(data.leastSignificantBits)
            bb.array            
        }
    }
    
}