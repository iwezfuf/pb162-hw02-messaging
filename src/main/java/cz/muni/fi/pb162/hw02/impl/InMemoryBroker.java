package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryBroker implements Broker {
    private final List<Message> data = new ArrayList<>();
    private final AtomicLong counter = new AtomicLong();
    @Override
    public Collection<String> listTopics() {
        Set<String> topics = new HashSet<>();
        for (var message : data) {
            topics.addAll(message.topics());
        }
        return topics;
    }

    @Override
    public Collection<Message> push(Collection<Message> messages) {
        List<Message> messagesWithId = new ArrayList<>();
        for (var message : messages) {
            messagesWithId.add(new MessageImpl(counter.getAndIncrement(), message.topics(), message.data()));
        }
        data.addAll(messagesWithId);
        return messagesWithId;
    }

    @Override
    public Collection<Message> poll(Map<String, Long> offsets, int num, Collection<String> topics) {
        Set<Message> messages = new HashSet<>();

        Map<String, Integer> topicsCounts = new HashMap<>();
        for (var topic : topics) {
            topicsCounts.put(topic, 0);
        }
        for (var message : data) {
            for (var topic : topics) {
                if (message.topics().contains(topic)
                        && topicsCounts.get(topic) < num
                        && (!offsets.containsKey(topic) || message.id() > offsets.get(topic))) {
                    messages.add(message);
                    for (var messageTopic : message.topics()) {
                        if (topics.contains(messageTopic)) {
                            topicsCounts.put(messageTopic, topicsCounts.get(messageTopic) + 1);
                        }
                    }
                    break;
                }
            }
        }
        return messages;
    }
}
