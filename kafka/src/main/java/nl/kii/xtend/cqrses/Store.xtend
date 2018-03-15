package nl.kii.xtend.cqrses

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import java.util.UUID

interface Store {

    def <AR extends AggregateRoot> void load(UUID id, Class<AR> type, Handler<AsyncResult<Buffer>> result)

}
