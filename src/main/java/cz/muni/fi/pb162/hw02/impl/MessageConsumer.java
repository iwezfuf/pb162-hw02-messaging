package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumer implements Consumer {
    private final Broker broker;
    private Map<String, Long> offsets = new HashMap<>();

    /**
     * Constructor
     * @param broker it's broker
     */
    public MessageConsumer(Broker broker) {
        this.broker = broker;
    }
    @Override
    public Broker getBroker() {
        return broker;
    }

    @Override
    public Collection<String> listTopics() {
        return broker.listTopics();
    }

    @Override
    public Collection<Message> consume(int num, String... topics) {
        List<Message> messages = new ArrayList<>(consume(offsets, num, topics));
        messages.sort(Comparator.comparingLong(Message::id));
        for (var topic : topics) {
            int count = 0;
            for (var message : messages) {
                if (message.topics().contains(topic)) {
                    offsets.put(topic, Math.max(message.id(), offsets.getOrDefault(topic, 0L)));
                    if (++count == num) {
                        break;
                    }
                }
            }
        }
        return messages;
    }

    @Override
    public Collection<Message> consume(Map<String, Long> offsets, int num, String... topics) {
        return broker.poll(offsets, num, Arrays.asList(topics));
    }

    @Override
    public Map<String, Long> getOffsets() {
        return new HashMap<>(offsets);
    }

    @Override
    public void setOffsets(Map<String, Long> offsets) {
        this.offsets = offsets;
    }

    @Override
    public void clearOffsets() {
        offsets = new HashMap<>();
    }

    @Override
    public void updateOffsets(Map<String, Long> offsets) {
        this.offsets.putAll(offsets);
    }
}
