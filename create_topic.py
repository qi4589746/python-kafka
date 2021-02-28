from confluent_kafka.admin import AdminClient, NewTopic


if __name__ == "__main__":
    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })

    topic_list = []
    topic_list.append(NewTopic("example_topic1", 1, 1))
    admin_client.create_topics(topic_list)
    