package DistrictNodeManager;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.ReceiveBuilder;
import common.BaseServiceActor;
import common.events.UserLifecycleEvent;
import common.events.NodeLifecycleEvent;
import common.kafka.KafkaService;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages grid nodes (create/update/delete). Results are published as NodeLifecycleEvents
 * Authorized users are tracked by consuming the user-lifecycle topic
 * In-memory state is rebuilt by consuming the node-lifecycle output topic on startup
 */
public class DistrictNodeManager extends BaseServiceActor<DistrictNodeManager.Command> {

    private static final List<String> VALID_NODE_TYPES = List.of("producer", "consumer", "accumulator");
    private static final String OUTPUT_TOPIC = "kafka.topics.node-lifecycle";

    // in-memory node store, keyed by nodeId
    private final Map<String, Node> nodes = new HashMap<>();

    // set of user IDs that are allowed to create/own nodes
    private final java.util.Set<String> authorizedUsers = new java.util.HashSet<>();

    /**
     * Tracks the highest ID assigned so far, so new IDs are always unique
     * Starts at 1 so the first generated ID is n_2 (node 1 in Cooja is the RPL Border Router)
     * Synced with the current max in {@code nodes} whenever a create is processed
     */
    private int lastAssignedNumericNodeId = 1;

    private DistrictNodeManager(ActorContext<Command> context, KafkaService kafka) {
        super(context, kafka);
    }

    /**
     * Factory method used by {@link common.ServiceLauncher} to create this actor
     */
    public static Behavior<Command> create(KafkaService kafka) {
        return BaseServiceActor.create(context -> new DistrictNodeManager(context, kafka));
    }

    @Override
    protected String getServiceName() {
        return "DistrictNodeManager";
    }

    @Override
    protected void addMessageHandlers(ReceiveBuilder<Command> builder) {
        builder.onMessage(ProcessNodeRequest.class, this::onProcessNodeRequest);
        builder.onMessage(ProcessUserLifecycle.class, this::onProcessUserLifecycle);
        builder.onMessage(ProcessNodeLifecycle.class, this::onProcessNodeLifecycle);
        builder.onMessage(GetNode.class, this::onGetNode);
        builder.onMessage(GetAllNodes.class, this::onGetAllNodes);
        builder.onMessage(GetNodeCount.class, this::onGetNodeCount);
    }

    private Behavior<Command> onProcessNodeRequest(@NotNull ProcessNodeRequest command) {
        NodeRequestEvent event = command.event;
        String eventType = event.eventType();

        log.info("Processing request: type={}, eventId={}", eventType, event.eventId());

        switch (eventType) {
            case "NodeCreateRequest" -> handleCreate(event);
            case "NodeUpdateRequest" -> handleUpdate(event);
            case "NodeDeleteRequest" -> handleDelete(event);
            default -> {
                log.warn("Unknown request type: {}", eventType);
                sendRejection(event, "Unknown request type");
            }
        }

        return this;
    }

    /**
     * Keeps the authorized-user set consistent with AccountService lifecycle events
     */
    private Behavior<Command> onProcessUserLifecycle(@NotNull ProcessUserLifecycle command) {
        UserLifecycleEvent event = command.event;
        String eventType = event.eventType();
        String userId = event.payload() != null ? event.payload().userId() : null;

        log.info("Processing user lifecycle event: type={}, userId={}", eventType, userId);

        if (!event.success() || userId == null || userId.isBlank()) {
            log.warn("User lifecycle event missing userId or not successful, ignoring");
            return this;
        }

        switch (eventType) {
            case "UserRegistered", "UserUpdated" -> {
                authorizedUsers.add(userId);
                log.info("User added to authorized set: {}, total: {}", userId, authorizedUsers.size());
            }
            case "UserDeleted" -> {
                authorizedUsers.remove(userId);
                log.info("User removed from authorized set: {}, total: {}", userId, authorizedUsers.size());
            }
            default -> log.debug("Ignoring user lifecycle event type: {}", eventType);
        }

        return this;
    }

    /**
     * Keeps the in-memory node map consistent with committed node lifecycle events
     */
    private Behavior<Command> onProcessNodeLifecycle(@NotNull ProcessNodeLifecycle command) {
        NodeLifecycleEvent event = command.event;
        String eventType = event.eventType();
        String nodeId = event.payload() != null ? event.payload().nodeId() : null;

        log.info("Processing node lifecycle event: type={}, nodeId={}", eventType, nodeId);

        if (!event.success() || nodeId == null || nodeId.isBlank()) {
            return this;
        }

        switch (eventType) {
            case "NodeCreated" -> {
                Node node = new Node(nodeId, event.payload().userId(), event.payload().districtId(), event.payload().nodeType());
                nodes.put(nodeId, node);
                int numericId = extractNumericNodeId(nodeId);
                if (numericId > 0) {
                    lastAssignedNumericNodeId = Math.max(lastAssignedNumericNodeId, numericId);
                }
                log.info("Node added to map: {}", node);
            }
            case "NodeUpdated" -> {
                Node node = new Node(nodeId, event.payload().userId(), event.payload().districtId(), event.payload().nodeType());
                nodes.put(nodeId, node);
                log.info("Node updated in map: {}", node);
            }
            case "NodeDeleted" -> {
                nodes.remove(nodeId);
                log.info("Node removed from map: nodeId={}", nodeId);
            }
            default -> log.debug("Ignoring node lifecycle event type: {}", eventType);
        }

        return this;
    }

    private void handleCreate(@NotNull NodeRequestEvent event) {
        String userId = event.payload().userId();
        String districtId = event.payload().districtId();
        String nodeType = event.payload().nodeType();

        if (isEmpty(userId)) {
            log.warn("Create failed: missing userId");
            sendRejection(event, "Missing required field: userId");
            return;
        }

        if (isEmpty(districtId)) {
            log.warn("Create failed: missing districtId");
            sendRejection(event, "Missing required field: districtId");
            return;
        }

        if (isEmpty(nodeType)) {
            log.warn("Create failed: missing nodeType");
            sendRejection(event, "Missing required field: nodeType");
            return;
        }

        if (!VALID_NODE_TYPES.contains(nodeType.toLowerCase())) {
            log.warn("Create failed: invalid nodeType '{}'. Valid types: {}", nodeType, VALID_NODE_TYPES);
            sendRejection(event, "Invalid nodeType. Valid types: producer, consumer, accumulator");
            return;
        }

        if (!authorizedUsers.contains(userId)) {
            log.warn("Create failed: unauthorized user {}", userId);
            sendRejection(event, "Unauthorized user");
            return;
        }

        // Sync counter with current nodes in case state was rebuilt from Kafka
        lastAssignedNumericNodeId = Math.max(lastAssignedNumericNodeId, findMaxNumericNodeId());
        int nextId = ++lastAssignedNumericNodeId;
        String nodeId = "n_" + nextId;

        Node newNode = new Node(nodeId, userId, districtId, nodeType.toLowerCase());
        nodes.put(nodeId, newNode);

        log.info("Node created: {}", newNode);
        sendEvent(OUTPUT_TOPIC, common.events.NodeLifecycleEvent.createNodeCreated(event.eventId(), newNode));
    }

    private void handleUpdate(@NotNull NodeRequestEvent event) {
        String nodeId = event.payload().nodeId();
        String userId = event.payload().userId();
        String districtId = event.payload().districtId();
        String nodeType = event.payload().nodeType();

        if (isEmpty(nodeId)) {
            log.warn("Update failed: missing nodeId");
            sendRejection(event, "Missing required field: nodeId");
            return;
        }

        if (!nodes.containsKey(nodeId)) {
            log.warn("Update failed: node {} not found", nodeId);
            sendRejection(event, "Node not found");
            return;
        }

        Node existingNode = nodes.get(nodeId);

        if (!isEmpty(userId) && !userId.equals(existingNode.userId())) {
            log.warn("Update failed: user {} does not own node {}", userId, nodeId);
            sendRejection(event, "User does not own this node");
            return;
        }

        if (!isEmpty(nodeType) && !VALID_NODE_TYPES.contains(nodeType.toLowerCase())) {
            log.warn("Update failed: invalid nodeType '{}'. Valid types: {}", nodeType, VALID_NODE_TYPES);
            sendRejection(event, "Invalid nodeType. Valid types: producer, consumer, accumulator");
            return;
        }

        // Keep existing values for fields not provided in the request
        String newDistrictId = isEmpty(districtId) ? existingNode.districtId() : districtId;
        String newNodeType = isEmpty(nodeType) ? existingNode.nodeType() : nodeType.toLowerCase();

        Node updatedNode = new Node(nodeId, existingNode.userId(), newDistrictId, newNodeType);
        nodes.put(nodeId, updatedNode);

        log.info("Node updated: {}", updatedNode);
        sendEvent(OUTPUT_TOPIC, common.events.NodeLifecycleEvent.createNodeUpdated(event.eventId(), updatedNode));
    }

    private void handleDelete(@NotNull NodeRequestEvent event) {
        String nodeId = event.payload().nodeId();
        String userId = event.payload().userId();

        if (isEmpty(nodeId)) {
            log.warn("Delete failed: missing nodeId");
            sendRejection(event, "Missing required field: nodeId");
            return;
        }

        if (!nodes.containsKey(nodeId)) {
            log.warn("Delete failed: node {} not found", nodeId);
            sendRejection(event, "Node not found");
            return;
        }

        Node existingNode = nodes.get(nodeId);

        if (!isEmpty(userId) && !userId.equals(existingNode.userId())) {
            log.warn("Delete failed: user {} does not own node {}", userId, nodeId);
            sendRejection(event, "User does not own this node");
            return;
        }

        nodes.remove(nodeId);

        log.info("Node deleted: nodeId={}", nodeId);
        sendEvent(OUTPUT_TOPIC, common.events.NodeLifecycleEvent.createNodeDeleted(event.eventId(), nodeId));
    }

    private Behavior<Command> onGetNode(@NotNull GetNode command) {
        Node node = nodes.get(command.nodeId);
        command.replyTo.tell(new NodeResponse(node));
        return this;
    }

    private Behavior<Command> onGetAllNodes(@NotNull GetAllNodes command) {
        command.replyTo.tell(new AllNodesResponse(new HashMap<>(nodes)));
        return this;
    }

    private Behavior<Command> onGetNodeCount(@NotNull GetNodeCount command) {
        command.replyTo.tell(new NodeCountResponse(nodes.size()));
        return this;
    }

    private boolean isEmpty(String value) {
        return value == null || value.isBlank();
    }

    private void sendRejection(NodeRequestEvent event, String reason) {
        sendEvent(OUTPUT_TOPIC, common.events.NodeLifecycleEvent.createRejected(event.eventId(), event.eventType(), reason));
    }

    /**
     * Returns the largest numeric suffix among IDs of the form "n_<number>"
     * Non-matching IDs are ignored. Returns 1 if no matching IDs exist
     */
    private int findMaxNumericNodeId() {
        int max = 1;
        for (String id : nodes.keySet()) {
            if (id == null || !id.startsWith("n_")) continue;
            String suffix = id.substring(2);
            try {
                int n = Integer.parseInt(suffix);
                if (n > max) max = n;
            } catch (NumberFormatException ignored) {
            }
        }
        return max;
    }

    /**
     * Extracts the numeric part from a node ID of the form "n_<number>"
     * Returns 0 if the ID does not match the expected format
     */
    private int extractNumericNodeId(String nodeId) {
        if (nodeId == null || !nodeId.startsWith("n_")) {
            return 0;
        }
        try {
            return Integer.parseInt(nodeId.substring(2));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    protected Behavior<Command> onPreRestart() {
        log.warn("DistrictNodeManager restarting. Nodes in memory: {}", nodes.size());
        return super.onPreRestart();
    }

    @Override
    protected Behavior<Command> onPostStop() {
        log.info("DistrictNodeManager stopped. Total nodes managed: {}", nodes.size());
        return super.onPostStop();
    }

    public interface Command {
    }

    public record ProcessNodeRequest(NodeRequestEvent event) implements Command {
    }

    /** Carries a UserLifecycleEvent so the actor can keep the authorized-user set up to date */
    public record ProcessUserLifecycle(UserLifecycleEvent event) implements Command {
    }

    /** Carries a NodeLifecycleEvent so the actor can keep the in-memory node map up to date */
    public record ProcessNodeLifecycle(NodeLifecycleEvent event) implements Command {
    }

    // query commands (used internally/for tests)
    public record GetNode(String nodeId, ActorRef<NodeResponse> replyTo) implements Command {
    }

    public record GetAllNodes(ActorRef<AllNodesResponse> replyTo) implements Command {
    }

    public record GetNodeCount(ActorRef<NodeCountResponse> replyTo) implements Command {
    }

    public record NodeResponse(Node node) {
    }

    public record AllNodesResponse(Map<String, Node> nodes) {
    }

    public record NodeCountResponse(int count) {
    }
}
