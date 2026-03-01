package BillingService;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.ReceiveBuilder;
import common.BaseServiceActor;
import common.events.MeasurementEvent;
import common.kafka.KafkaService;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates energy measurements into fixed-size time windows and publishes usage records.
 * Each window is keyed by (userId, districtId, windowStart). For each incoming measurement
 * the running totals are updated and a snapshot UsageRecordCreated event is published.
 * In-memory state is rebuilt by also consuming the usage-records output topic.
 */
public class BillingService extends BaseServiceActor<BillingService.Command> {

    private static final String OUTPUT_TOPIC = "kafka.topics.usage-records";

    private static final Duration WINDOW_SIZE = Duration.ofSeconds(180);

    /** Cost per unit of net consumed energy (consumed - produced). */
    private static final double PRICE_PER_UNIT = 0.0005;

    // aggregation buckets keyed by (userId, districtId, windowStart)
    private final Map<BucketKey, Bucket> buckets = new HashMap<>();

    private BillingService(ActorContext<Command> context, KafkaService kafka) {
        super(context, kafka);
    }

    public static Behavior<Command> create(KafkaService kafka) {
        return BaseServiceActor.create(context -> new BillingService(context, kafka));
    }

    @Override
    protected String getServiceName() {
        return "BillingService";
    }

    @Override
    protected void addMessageHandlers(ReceiveBuilder<Command> builder) {
        builder.onMessage(ProcessMeasurementEvent.class, this::onProcessMeasurementEvent);
        builder.onMessage(ProcessUsageRecord.class, this::onProcessUsageRecord);
    }

    private Behavior<Command> onProcessMeasurementEvent(@NotNull ProcessMeasurementEvent command) {
        MeasurementEvent event = command.event;

        if (!event.success() || event.payload() == null) {
            return this;
        }

        if (!"MeasurementReported".equals(event.eventType())) {
            return this;
        }

        String eventId = event.eventId();
        MeasurementEvent.Payload p = event.payload();

        if (isEmpty(p.userId()) || isEmpty(p.districtId()) || isEmpty(p.measuredAt()) || isEmpty(p.nodeType())) {
            sendEvent(OUTPUT_TOPIC, UsageRecordEvent.createRejected(eventId, "Missing required measurement fields for billing"));
            return this;
        }

        String nodeType = p.nodeType().toLowerCase();

        // Accumulator measurements are not billed
        if ("accumulator".equals(nodeType)) {
            log.debug("Ignoring accumulator measurement for billing: eventId={}", eventId);
            return this;
        }

        // MeasurementService already validated sign vs node type. This is a secondary check
        double v = p.value();

        if ("producer".equals(nodeType) && v < 0) {
            sendEvent(OUTPUT_TOPIC, UsageRecordEvent.createRejected(eventId, "Invalid producer measurement (negative)"));
            return this;
        }

        if ("consumer".equals(nodeType) && v > 0) {
            sendEvent(OUTPUT_TOPIC, UsageRecordEvent.createRejected(eventId, "Invalid consumer measurement (positive)"));
            return this;
        }

        long measuredAt = parseTimestampMillis(p.measuredAt());
        long windowStart = floorToWindowStart(measuredAt, WINDOW_SIZE.toMillis());
        long windowEnd = windowStart + WINDOW_SIZE.toMillis();

        BucketKey key = new BucketKey(p.userId(), p.districtId(), windowStart);
        Bucket bucket = buckets.computeIfAbsent(key, bucketKey -> new Bucket(windowStart, windowEnd));

        // Compute running totals without updating the bucket
        double tempProduced = bucket.produced;
        double tempConsumed = bucket.consumed;
        if ("producer".equals(nodeType)) {
            tempProduced += v;
        } else if ("consumer".equals(nodeType)) {
            tempConsumed += Math.abs(v);
        } else {
            log.debug("Ignoring measurement for billing: nodeType={}, eventId={}", nodeType, eventId);
        }

        double produced = tempProduced;
        double consumed = tempConsumed;
        double netBalance = produced - consumed;
        double netConsumed = Math.max(0, consumed - produced);
        double cost = netConsumed * PRICE_PER_UNIT;

        String usageEventId = "u_" + p.userId() + "_" + p.districtId() + "_" + windowStart;
        sendEvent(OUTPUT_TOPIC, UsageRecordEvent.createUsageRecord(
                usageEventId,
                p.userId(),
                p.districtId(),
                windowStart,
                windowEnd,
                produced,
                consumed,
                netBalance,
                cost
        ));

        log.info("Usage record sent: userId={}, districtId={}, windowStart={}, produced={}, consumed={}, cost={}",
                p.userId(), p.districtId(), windowStart, produced, consumed, cost);

        return this;
    }

    /**
     * Keeps bucket state consistent by consuming committed usage records from Kafka.
     */
    private Behavior<Command> onProcessUsageRecord(@NotNull ProcessUsageRecord command) {
        UsageRecordEvent event = command.event;

        if (!event.success() || event.payload() == null) {
            return this;
        }

        if (!"UsageRecordCreated".equals(event.eventType())) {
            return this;
        }

        UsageRecordEvent.Payload p = event.payload();

        if (isEmpty(p.userId()) || isEmpty(p.districtId())) {
            log.warn("Invalid usage record event: missing fields");
            return this;
        }

        BucketKey key = new BucketKey(p.userId(), p.districtId(), p.windowStart());
        Bucket bucket = buckets.computeIfAbsent(key, (bucketKey) -> new Bucket(p.windowStart(), p.windowEnd()));

        bucket.produced = p.produced();
        bucket.consumed = p.consumed();

        log.info("Bucket updated from usage record: userId={}, districtId={}, windowStart={}, produced={}, consumed={}",
                p.userId(), p.districtId(), p.windowStart(), p.produced(), p.consumed());

        return this;
    }

    private boolean isEmpty(String value) {
        return value == null || value.isBlank();
    }

    private long parseTimestampMillis(String ts) {
        try {
            return Long.parseLong(ts);
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private long floorToWindowStart(long timestampMillis, long windowSizeMillis) {
        return (timestampMillis / windowSizeMillis) * windowSizeMillis;
    }

    @Override
    protected Behavior<Command> onPreRestart() {
        log.warn("BillingService restarting. Buckets in memory: {}", buckets.size());
        return super.onPreRestart();
    }

    @Override
    protected Behavior<Command> onPostStop() {
        log.info("BillingService stopped. Buckets managed: {}", buckets.size());
        return super.onPostStop();
    }

    public interface Command {
    }

    public record ProcessMeasurementEvent(common.events.MeasurementEvent event) implements Command {
    }

    public record ProcessUsageRecord(UsageRecordEvent event) implements Command {
    }


    private record BucketKey(String userId, String districtId, long windowStart) {
    }

    private static class Bucket {
        final long windowStart;
        final long windowEnd;
        double produced;
        double consumed;

        Bucket(long windowStart, long windowEnd) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.produced = 0.0;
            this.consumed = 0.0;
        }
    }
}
