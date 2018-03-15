package nl.kii.xtend.cqrses

import java.util.Map

/**
 * Represents a Message carrying a command as its payload. These messages carry an intention to change application
 * state.
 *
 * @param <C> The type of payload contained in the message
 */
interface CommandMessage<C> extends Message<C> {
    
    /**
     * Returns the name of the command to execute. This is an indication of what should be done, using the payload as
     * parameter.
     *
     * @return the name of the command
     */
    def String getCommandName()

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
    override CommandMessage<C> withMetaData(Map<String, ?> metadata)
    
    /**
     * Returns a copy of this Message with it MetaData merged with the given <code>metaData</code>. The payload
     * remains unchanged.
     *
     * @param metadata The MetaData to merge with
     * @return a copy of this message with the given MetaData
     */
    override CommandMessage<C> andMetaData(Map<String, ?> metadata)

}