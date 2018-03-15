package nl.kii.xtend.cqrses

import io.vertx.core.Future
import java.util.UUID
import java.util.Map

interface AsyncRepository {
   
    def <A extends AggregateRoot> Future<A> load(UUID id, Class<A> type)

    def <A extends AggregateRoot> Future<Void> save(A object, Map<String, ?> metadata) 

}