package common.kafka;

import akka.Done;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.SendProducer;
import akka.stream.RestartSettings;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import common.events.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Function;

/**
 * Wraps Alpakka Kafka producer and consumer setup.
 * Any service can use this class to send events to Kafka or start a consuming stream.
 */
public class KafkaService {

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);

    private final SendProducer<String, String> producer;
    private final String serviceName;

    /**
     * @param system      Akka actor system
     * @param serviceName name of the owning service, used in log messages
     */
    public KafkaService(ActorSystem<?> system, String serviceName) {
        this.serviceName = serviceName;

        Config config = ConfigFactory.load();
        String bootstrapServers = config.getString("kafka.bootstrap-servers");

        ProducerSettings<String, String> producerSettings = ProducerSettings
                .create(system, new StringSerializer(), new StringSerializer())
                .withBootstrapServers(bootstrapServers);

        this.producer = new SendProducer<>(producerSettings, system);

        log.info("KafkaService initialized for service: {}", serviceName);
    }

    /**
     * Starts an Alpakka Kafka consumer stream that forwards each message to the given actor.
     * The stream restarts automatically on failure with exponential backoff.
     *
     * @param <C>                  command type the actor receives
     * @param system               Akka actor system
     * @param serviceName          name of the owning service (for logging)
     * @param consumerConfigPrefix config path prefix for consumer settings
     * @param topicConfigKey       config path for the topic name
     * @param actor                target actor that receives parsed commands
     * @param messageFactory       converts a raw JSON string into an actor command
     */
    public static <C> void startConsumer(ActorSystem<?> system, String serviceName, String consumerConfigPrefix, String topicConfigKey, ActorRef<C> actor, Function<String, C> messageFactory) {
        Config config = ConfigFactory.load();
        String bootstrapServers = config.getString("kafka.bootstrap-servers");
        String topic = config.getString(topicConfigKey);
        String groupId = config.getString(consumerConfigPrefix + ".group-id");
        String autoOffsetReset = config.getString(consumerConfigPrefix + ".auto-offset-reset");

        ConsumerSettings<String, String> consumerSettings = ConsumerSettings
                .create(system, new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers(bootstrapServers)
                .withGroupId(groupId)
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        // Restart the consumer stream on failure: backoff 3s–30s, max 5 restarts per minute
        RestartSettings restartSettings = RestartSettings
                .create(Duration.ofSeconds(3), Duration.ofSeconds(30), 0.2)
                .withMaxRestarts(5, Duration.ofMinutes(1));

        RestartSource.onFailuresWithBackoff(restartSettings, () ->
                Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
                        .map(record -> processRecord(record, actor, messageFactory, serviceName))
        ).runWith(Sink.ignore(), system);

        log.info("Kafka consumer started for service: {}, topic: {}", serviceName, topic);
    }

    private static <C> @NotNull Done processRecord(ConsumerRecord<String, String> record, ActorRef<C> actor, Function<String, C> messageFactory, String serviceName) {
        try {
            log.debug("[{}] Received message: partition={}, offset={}", serviceName, record.partition(), record.offset());
            C message = messageFactory.apply(record.value());
            actor.tell(message);
            return Done.getInstance();
        } catch (Exception e) {
            // Log the error but do not crash the stream
            log.error("[{}] Error processing Kafka record: {}", serviceName, record.value(), e);
            return Done.getInstance();
        }
    }

    /**
     * Sends an event to a Kafka topic asynchronously.
     * The event ID is used as the record key, while the value is the JSON representation.
     *
     * @param topicConfigKey config path for the topic name
     * @param event          event to send
     */
    public void sendEvent(String topicConfigKey, Event event) {
        try {
            Config config = ConfigFactory.load();
            String topic = config.getString(topicConfigKey);
            String json = event.toJson();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.eventId(), json);

            producer.send(record)
                    .thenAccept(metadata -> log.info("[{}] Event sent: type={}, id={}, partition={}, offset={}",
                            serviceName, event.eventType(), event.eventId(), metadata.partition(), metadata.offset()))
                    .exceptionally(ex -> {
                        log.error("[{}] Failed to send event: type={}, id={}", serviceName, event.eventType(), event.eventId(), ex);
                        return null;
                    });
        } catch (Exception e) {
            log.error("[{}] Error preparing event for Kafka: {}", serviceName, event, e);
        }
    }
}
