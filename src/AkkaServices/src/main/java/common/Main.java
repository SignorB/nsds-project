package common;

/**
 * Entry point that launches the appropriate service based on the first command-line argument.
 */
public class Main {

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsageAndExit();
        }

        String serviceName = args[0].toLowerCase().trim();

        switch (serviceName) {
            case "account-service":
            case "account":
                AccountService.AccountServiceApp.main();
                break;
            case "billing-service":
            case "billing":
                BillingService.BillingServiceApp.main();
                break;
            case "district-node-manager":
            case "node-manager":
            case "district":
                DistrictNodeManager.DistrictNodeManagerApp.main();
                break;
            case "measurement-service":
            case "measurement":
                MeasurementService.MeasurementServiceApp.main();
                break;
            case "ping-pong-service":
            case "ping-pong":
            case "pingpong":
                PingPongService.PingPongServiceApp.main();
                break;
            default:
                System.err.println("Unknown service: " + serviceName);
                printUsageAndExit();
        }
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: java -jar akka-services.jar <service-name>");
        System.err.println();
        System.err.println("Available services:");
        System.err.println("  account-service        - User management service");
        System.err.println("  billing-service        - Billing and usage tracking service");
        System.err.println("  district-node-manager  - Node management service");
        System.err.println("  measurement-service    - Measurement processing service");
        System.err.println("  ping-pong-service      - Ping pong test service");
        System.exit(1);
    }
}
