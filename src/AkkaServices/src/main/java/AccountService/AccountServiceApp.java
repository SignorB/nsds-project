package AccountService;

import common.ServiceLauncher;
import common.TopicSubscription;
import common.events.UserLifecycleEvent;

import java.util.List;
import java.util.UUID;

/**
 * Entry point for AccountService. Subscribes to the user-requests topic for incoming commands
 * and to the user-lifecycle topic to maintain in-memory state across instances.
 */
public class AccountServiceApp {

    public static void main() {
        if (System.getProperty("INSTANCE_ID") == null) {
            System.setProperty("INSTANCE_ID", UUID.randomUUID().toString());
        }

        List<TopicSubscription<AccountService.Command>> subscriptions = List.of(

                TopicSubscription.create(
                        "kafka.account-consumer",
                        "kafka.topics.user-requests",
                        json -> new AccountService.ProcessUserRequest(UserRequestEvent.fromJson(json))
                ),

                // also consume the output topic to keep in-memory state consistent
                TopicSubscription.create(
                        "kafka.user-lifecycle-consumer",
                        "kafka.topics.user-lifecycle",
                        json -> new AccountService.ProcessUserLifecycle(UserLifecycleEvent.fromJson(json))
                )
        );

        ServiceLauncher.launch(
                "AccountService",
                "account-service",
                AccountService::create,
                subscriptions
        );
    }
}
