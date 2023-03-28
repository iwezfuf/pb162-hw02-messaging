package cz.muni.fi.pb162.hw02.mesaging.broker;

import java.util.Map;


/**
 * Messages as stored by Broker and delivered to Consumers
 */
public interface Message {
    /**
     * Unique identifier of this message.
     * The id also serves as an offset for broker.
     *
     * @return message id or null if this message was not stored yet
     */
    Long id();


    /**
     * Topic of this message
     *
     * @return destination topic associated with this message
     */
    String topic();

    /**
     * Data of this message
     *
     * @return data associated with this message
     */
    Map<String, Object> data();
}