package nl.kii.xtend.cqrses

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.Map
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import com.fasterxml.jackson.databind.module.SimpleModule

class GenericSerde<T> implements Serde<T> {

    override close() {}

    override configure(Map<String, ?> configs, boolean isKey) {}

    override deserializer() {
        new Deserializer<T> {
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
    }

    override serializer() {
        new Serializer<T>() {
            override close() {}

            override configure(Map<String, ?> configs, boolean isKey) {}

            override serialize(String topic, Object data) {
                val baos = new ByteArrayOutputStream
                val out = new ObjectOutputStream(baos)
                out.writeObject(data)
                baos.toByteArray => [
                    out.close
                    baos.close
                ]
            }
        }
    }

}

@FinalFieldsConstructor
class JsonSerde<T> implements Serde<T> {
    
    val mapper = new ObjectMapper => [
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
        val module = new SimpleModule
        registerModule(module)
    ]
    
    val Class<? extends T> type
    
    override close() {}
    
    override configure(Map<String, ?> configs, boolean isKey) {}
    
    override deserializer() {
        new Deserializer<T> {
            
            override close() {}
            
            override configure(Map<String, ?> configs, boolean isKey) {}
            
            override deserialize(String topic, byte[] data) {
                mapper.readValue(data, type)
            }
            
        }
    }
    
    override serializer() {
        new Serializer<T> {
            
            override close() {}
            
            override configure(Map<String, ?> configs, boolean isKey) {}
            
            override serialize(String topic, T data) {
                mapper.writeValueAsBytes(data)
            }
            
        }
    }
    
}
