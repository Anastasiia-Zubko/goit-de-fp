from kafka.admin import KafkaAdminClient, NewTopic

# Kafka configs
kafka_bootstrap_servers = "77.81.230.104:9092"
kafka_user = "admin"
kafka_password = "VawEzo1ikLtrA8Ug8THa"
kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "PLAIN"

# Topics
NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1
SUFFIX = "zubko"
EVENTS_TOPIC = f'athlete_event_results_{SUFFIX}'
AGG_OUTPUT_TOPIC = f'athlete_enriched_agg_{SUFFIX}'
TOPICS = [EVENTS_TOPIC, AGG_OUTPUT_TOPIC]


def create_kafka_admin() -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=kafka_bootstrap_servers.split(','),
        security_protocol=kafka_security_protocol,
        sasl_mechanism=kafka_sasl_mechanism,
        sasl_plain_username=kafka_user,
        sasl_plain_password=kafka_password
    )


def main():
    admin_client = create_kafka_admin()

    new_topics = [
        NewTopic(name=name, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
        for name in TOPICS
    ]

    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"Topics created: {[t.name for t in new_topics]}")
    except Exception as e:
        print(f"Error creating topics: {e}")

    print(f"Existing topics: {admin_client.list_topics()}")
    admin_client.close()


if __name__ == "__main__":
    main()
