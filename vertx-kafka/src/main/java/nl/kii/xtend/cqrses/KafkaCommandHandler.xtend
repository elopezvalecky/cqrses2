package nl.kii.xtend.cqrses

import java.util.UUID
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor

@FinalFieldsConstructor
abstract class KafkaCommandHandler implements Transformer<UUID, CommandMessage<? extends Command>, KeyValue<UUID, AggregateRoot>> {

    val protected String storeName
    var protected KeyValueStore<UUID, AggregateRoot> store

    override init(ProcessorContext context) {
        store = context.getStateStore(storeName) as KeyValueStore<UUID, AggregateRoot>
    }

    override transform(UUID key, CommandMessage<? extends Command> value) {
        new KeyValue(key, process(value.payload))
    }

    override punctuate(long timestamp) {}

    override close() {}

    def AggregateRoot process(Command command)
}
