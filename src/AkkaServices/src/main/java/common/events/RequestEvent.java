package common.events;

/**
 * Interface for events that request an operation from a service.
 * The payload contains the data needed to perform the operation.
 */
public interface RequestEvent extends Event {

    Object payload();
}
