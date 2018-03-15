package nl.kii.xtend.cqrses

import java.lang.reflect.AccessibleObject
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Member
import java.lang.reflect.Modifier
import java.security.AccessController
import java.security.PrivilegedAction
import java.util.Iterator
import java.util.List
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import java.util.UUID

@FinalFieldsConstructor
class AggregateRootTransformer implements Transformer<Pair<String, UUID>, List<Event>, KeyValue<UUID, AggregateRoot>> {

    val private String storeName

    private ProcessorContext context;
    private KeyValueStore<UUID, AggregateRoot> store

    override init(ProcessorContext context) {
        this.context = context
        store = context.getStateStore(storeName) as KeyValueStore<UUID, AggregateRoot>
    }

    override transform(Pair<String, UUID> pair, List<Event> value) {
        val instance = getInstance(Class.forName(pair.key) as Class<AggregateRoot>, value.iterator)
        store.put(pair.value, instance)
        new KeyValue(pair.value, instance)
    }

    override punctuate(long timestamp) {}

    override close() {}

    def private <AR extends AggregateRoot> getInstance(Class<AR> aggregateClass, Iterator<Event> events) {
        try {
            val constructor = if (events === null) {
                    aggregateClass.getDeclaredConstructor
                } else {
                    aggregateClass.getDeclaredConstructor(Iterator)
                }
            if (!isAccessible(constructor)) {
                AccessController.doPrivileged(new PrivilegedAction {
                    override run() {
                        constructor.setAccessible(true)
                        Void
                    }
                })
            }
            if (events === null) {
                constructor.newInstance
            } else {
                constructor. newInstance(events)
            }
        } catch (InstantiationException e) {
            throw new UnsupportedOperationException('''The aggregate [«aggregateClass.simpleName»] does not have a suitable no-arg constructor.''', e)
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException('''The aggregate no-arg constructor of the aggregate [«aggregateClass.simpleName»] is not accessible. Please ensure that the constructor is public or that the Security Manager allows access through reflection.''', e)
        } catch (InvocationTargetException e) {
            throw new UnsupportedOperationException('''The no-arg constructor of [«aggregateClass.simpleName»] threw an exception on invocation.''', e)
        }
    }

    def private isAccessible(AccessibleObject member) {
        return member.isAccessible() || (Member.isInstance(member) && isNonFinalPublicMember(member as Member))
    }

    def private isNonFinalPublicMember(Member member) {
        return (Modifier.isPublic(member.getModifiers()) &&
            Modifier.isPublic(member.getDeclaringClass().getModifiers()) &&
            !Modifier.isFinal(member.getModifiers()))
    }

}
        