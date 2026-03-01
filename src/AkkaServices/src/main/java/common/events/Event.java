package common.events;

/**
 * Base interface for all events exchanged between services via Kafka.
 * Every event carries a unique ID, a type string, a timestamp, and the originating service ID,
 * and must be serializable to JSON.
 */
public interface Event {

    String eventId();

    /** Identifies the kind of event (es: "UserRegistered", "NodeCreated"). */
    String eventType();

    String timestamp();

    String serviceId();

    String toJson();
}
