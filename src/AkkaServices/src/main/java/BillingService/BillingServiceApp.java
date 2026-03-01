package BillingService;

import common.ServiceLauncher;
import common.TopicSubscription;

import java.util.List;
import java.util.UUID;

/**
 * Entry point for BillingService. Subscribes to measurement events and usage records.
 */
public class BillingServiceApp {

    public static void main() {
        if (System.getProperty("INSTANCE_ID") == null) {
            System.setProperty("INSTANCE_ID", UUID.randomUUID().toString());
        }

        List<TopicSubscription<BillingService.Command>> subscriptions = List.of(

                TopicSubscription.create(
                        "kafka.billing-consumer",
                        "kafka.topics.measurement-events",
                        json -> new BillingService.ProcessMeasurementEvent(common.events.MeasurementEvent.fromJson(json))
                ),

                // also consume the output topic to keep in-memory bucket state consistent
                TopicSubscription.create(
                        "kafka.billing-usage-consumer",
                        "kafka.topics.usage-records",
                        json -> new BillingService.ProcessUsageRecord(UsageRecordEvent.fromJson(json))
                )
        );

        ServiceLauncher.launch(
                "BillingService",
                "billing-service",
                BillingService::create,
                subscriptions
        );
    }
}
