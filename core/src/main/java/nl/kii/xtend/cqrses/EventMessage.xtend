package nl.kii.xtend.cqrses

import java.time.Instant
import java.util.Map

/**
 * Represents a Message wrapping an Event, which is represented by its payload. An Event is a representation of an
 * occurrence of an event (i.e. anything that happened any might be of importance to any other component) in the
 * application. It contains the data relevant for components that need to act based on that event.
 *
 * @param <E> The type of payload contained in this Message
 * @see DomainEventMessage
 * @since 1.0
 */
interface EventMessage<E extends Event> extends Message<E>{
    
    /**
     * Returns the timestamp of this event. The timestamp is set to the date and time the event was reported.
     *
     * @return the timestamp of this event.
     */
    def Instant getTimestamp()

    /**
     * Returns a copy of this Message with the given <code>metaData</code>. The payload remains unchanged.
     * 
     * While the implementation returned may be different than the implementation of <code>this</code>, implementations
     * must take special care in returning the same type of Message (e.g. EventMessage, DomainEventMessage) to prevent
     * errors further downstream.
     *
     * @param metadata The new MetaData for the Message
     * @return a copy of this message with the given MetaData
     */
    override EventMessage<E> withMetaData(Map<String, ?> metadata)
    
    /**
     * Returns a copy of this Message with it MetaData merged with the given <code>metaData</code>. The payload
     * remains unchanged.
     *
     * @param metadata The MetaData to merge with
     * @return a copy of this message with the given MetaData
     */
    override EventMessage<E> andMetaData(Map<String, ?> metadata)

}