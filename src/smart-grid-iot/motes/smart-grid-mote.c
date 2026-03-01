#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "contiki.h"
#include "lib/random.h"
#include "mqtt.h"
#include "net/routing/routing.h"
#include "sys/clock.h"
#include "sys/etimer.h"

#define BROKER_IP "fd00::1"
#define BROKER_PORT 1883

#define MIN_POWER 10
#define MAX_POWER 3000
#define UNDEFINED_POWER_VALUE 99999

// randomized intervals prevent synchronized transmissions across nodes
#define SEND_INTERVAL_MIN_SECONDS 10
#define SEND_INTERVAL_MAX_SECONDS 20
#define SEND_INTERVAL_GRANULARITY_MS 100
#define SEND_INTERVAL_MIN_MS 50
#define ACC_INTERVAL_MIN_SECONDS 5
#define ACC_INTERVAL_MAX_SECONDS 10

#if SEND_INTERVAL_MAX_SECONDS < SEND_INTERVAL_MIN_SECONDS
#error "Maximum interval must be greater than minimum interval"
#endif

#if SEND_INTERVAL_GRANULARITY_MS < SEND_INTERVAL_MIN_MS
#error "Granularity must be at least 50ms"
#endif

static struct mqtt_connection conn;

static char client_id[20];
static char pub_topic[50];
static char sub_topic[50];
static char sub_topic_acc[50];

typedef enum { STATE_INIT, STATE_CONNECTING, STATE_CONNECTED, STATE_DISCONNECTED } mqtt_state_t;

static mqtt_state_t mqtt_state = STATE_INIT;

typedef enum { PRODUCER, CONSUMER, ACCUMULATOR, NONE } mote_mode_t;

static mote_mode_t mote_mode = NONE;
static int is_active = 0;
static int subscribed = 0;
static int subscribed_acc = 0;
static int energy_state = 0;

PROCESS(mote_process, "Smart Grid Mote Process");
AUTOSTART_PROCESSES(&mote_process);

static int generate_random_power(void) {
    const int range = MAX_POWER - MIN_POWER + 1;
    const int random_value = random_rand() % range;
    return MIN_POWER + random_value;
}

static clock_time_t calculate_random_interval(int min, int max) {
    const uint32_t min_ms = min * 1000;
    const uint32_t max_ms = max * 1000;
    const uint32_t range_ms = max_ms - min_ms;

    // number of discrete steps within the range at the defined granularity
    const uint32_t num_steps = (range_ms / SEND_INTERVAL_GRANULARITY_MS) + 1;
    const uint32_t random_step = random_rand() % num_steps;
    const uint32_t interval_ms = min_ms + (random_step * SEND_INTERVAL_GRANULARITY_MS);

    clock_time_t ticks = (interval_ms * CLOCK_SECOND) / 1000;

    // enforce a lower bound to avoid excessively short intervals
    const clock_time_t min_ticks = (SEND_INTERVAL_MIN_MS * CLOCK_SECOND) / 1000;
    if (ticks < min_ticks) {
        ticks = min_ticks;
    }

    return ticks;
}

static void mqtt_event_handler(struct mqtt_connection* m, mqtt_event_t event, void* data) {
    switch (event) {
        case MQTT_EVENT_CONNECTED:
            printf("Connected to broker\n");
            mqtt_state = STATE_CONNECTED;
            break;

        case MQTT_EVENT_DISCONNECTED:
            printf("Disconnected from broker\n");
            mqtt_state = STATE_DISCONNECTED;
            subscribed = 0;
            break;

        case MQTT_EVENT_PUBLISH: {
            const struct mqtt_message* msg = data;
            if (msg != NULL && msg->payload_chunk != NULL) {
                char* command = (char*)msg->payload_chunk;
                printf("Received command: %.*s\n", msg->payload_chunk_length, command);

                if (strncmp(command, "START_PRODUCER", 14) == 0) {
                    is_active = 1;
                    mote_mode = PRODUCER;
                    printf("Now operating as PRODUCER\n");
                } else if (strncmp(command, "START_CONSUMER", 14) == 0) {
                    is_active = 1;
                    mote_mode = CONSUMER;
                    printf("Now operating as CONSUMER\n");
                } else if (strncmp(command, "START_ACCUMULATOR", 14) == 0) {
                    is_active = 1;
                    mote_mode = ACCUMULATOR;
                    printf("Now operating as ACCUMULATOR\n");
                } else if (strncmp(command, "STOP", 4) == 0) {
                    is_active = 0;
                    printf("Now IDLE\n");
                } else if (strncmp(command, "VALUE_", 6) == 0) {
                    // format: VALUE_<sign><magnitude>, where sign is 'P' or 'N'
                    const char sign = command[6];
                    const char* num_start = command + 7;
                    char* end;
                    long value = strtol(num_start, &end, 10);
                    if (end != num_start && value > 0 && (sign == 'P' || sign == 'N')) {
                        if (sign == 'N') {
                            value = -value;
                        }
                        energy_state += (int)value;
                        if (energy_state < 0) {
                            energy_state = 0;
                        }
                        printf("Added %ld to energy_state, now: %d\n", value, energy_state);
                    }
                }
            }
        } break;

        case MQTT_EVENT_SUBACK:
            printf("Subscription confirmed\n");
            break;

        case MQTT_EVENT_PUBACK:
            printf("Publish confirmed\n");
            break;

        case MQTT_EVENT_ERROR:
            printf("MQTT error occurred\n");
            mqtt_state = STATE_DISCONNECTED;
            subscribed = 0;
            break;

        default:
            break;
    }
}

PROCESS_THREAD(mote_process, ev, data) {
    static struct etimer timer;
    static char payload[20];

    PROCESS_BEGIN();

    // last byte of the link-layer address is used as a unique node identifier
    const int node_id = linkaddr_node_addr.u8[7];
    snprintf(client_id, sizeof(client_id), "node-%d", node_id);

    snprintf(pub_topic, sizeof(pub_topic), "grid/node/%s/measurements", client_id);
    snprintf(sub_topic, sizeof(sub_topic), "grid/node/%s/config", client_id);
    snprintf(sub_topic_acc, sizeof(sub_topic_acc), "grid/node/%s/acc", client_id);

    mqtt_register(&conn, &mote_process, client_id, mqtt_event_handler, 128);

    const clock_time_t initial_interval =
        calculate_random_interval(SEND_INTERVAL_MIN_SECONDS, SEND_INTERVAL_MAX_SECONDS);
    printf("Initial interval: %lu clock ticks (~%lu seconds)\n",
           (unsigned long)initial_interval,
           (unsigned long)(initial_interval / CLOCK_SECOND));
    etimer_set(&timer, initial_interval);

    while (1) {
        PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

        if (mqtt_state == STATE_CONNECTED) {
            if (!subscribed) {
                printf("Subscribing to: %s\n", sub_topic);
                mqtt_subscribe(&conn, NULL, sub_topic, MQTT_QOS_LEVEL_0);
                subscribed = 1;
            }

            if (is_active) {
                int power_value = 0;
                switch (mote_mode) {
                    case PRODUCER:
                        power_value = generate_random_power();
                        break;
                    case CONSUMER:
                        power_value = generate_random_power();
                        power_value = -power_value;
                        break;
                    case ACCUMULATOR:
                        if (!subscribed_acc) {
                            printf("Subscribing to: %s\n", sub_topic_acc);
                            mqtt_subscribe(&conn, NULL, sub_topic_acc, MQTT_QOS_LEVEL_0);
                            subscribed_acc = 1;
                        } else {
                            power_value = energy_state;
                        }
                        break;
                    case NONE:
                        power_value = UNDEFINED_POWER_VALUE;
                        break;
                }

                if (power_value != UNDEFINED_POWER_VALUE) {
                    snprintf(payload, sizeof(payload), "%d", power_value);
                    mqtt_publish(
                        &conn, NULL, pub_topic, (uint8_t*)payload, strlen(payload), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
                    printf("Published: %s\n", payload);
                }
            }
        } else if (mqtt_state == STATE_INIT || mqtt_state == STATE_DISCONNECTED) {
            printf("Connecting to broker...\n");
            mqtt_state = STATE_CONNECTING;
            mqtt_connect(&conn, BROKER_IP, BROKER_PORT, 120, MQTT_CLEAN_SESSION_ON);
        } else {
            printf("Waiting for connection...\n");
        }

        // accumulator nodes use a shorter interval due to higher update frequency requirements
        clock_time_t next_interval = 0;
        if (mote_mode == ACCUMULATOR) {
            next_interval = calculate_random_interval(ACC_INTERVAL_MIN_SECONDS, ACC_INTERVAL_MAX_SECONDS);
        } else {
            next_interval = calculate_random_interval(SEND_INTERVAL_MIN_SECONDS, SEND_INTERVAL_MAX_SECONDS);
        }
        printf("Next interval: %lu clock ticks (~%lu seconds)\n",
               (unsigned long)next_interval,
               (unsigned long)(next_interval / CLOCK_SECOND));

        etimer_set(&timer, next_interval);
    }

    PROCESS_END();
}
