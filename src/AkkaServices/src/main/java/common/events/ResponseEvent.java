package common.events;

/**
 * Interface for events that report the outcome of an operation.
 * {@code success} indicates whether the operation succeeded.
 * On failure, {@code reason} contains a short error description and {@code payload} is null.
 */
public interface ResponseEvent extends Event {

    Object payload();

    boolean success();

    String reason();
}
