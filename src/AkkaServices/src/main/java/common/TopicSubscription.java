package common;

import java.util.function.Function;

/**
 * Pairs a Kafka topic with a parser that converts incoming JSON messages into actor commands.
 *
 * @param <C> command type the actor receives
 */
public class TopicSubscription<C> {

    private final String consumerConfig;
    private final String topicConfigKey;
    private final Function<String, C> messageParser;

    private TopicSubscription(String consumerConfig, String topicConfigKey, Function<String, C> messageParser) {
        this.consumerConfig = consumerConfig;
        this.topicConfigKey = topicConfigKey;
        this.messageParser = messageParser;
    }

    /**
     * @param consumerConfig config path for consumer settings
     * @param topicConfigKey config path for the topic name
     * @param messageParser  converts a JSON string to a command
     * @param <C>            command type
     */
    public static <C> TopicSubscription<C> create(
            String consumerConfig,
            String topicConfigKey,
            Function<String, C> messageParser) {
        return new TopicSubscription<>(consumerConfig, topicConfigKey, messageParser);
    }

    public String getConsumerConfig() {
        return consumerConfig;
    }

    public String getTopicConfigKey() {
        return topicConfigKey;
    }

    public Function<String, C> getMessageParser() {
        return messageParser;
    }
}
