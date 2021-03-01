# 參考 https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html#ak-consumer
from confluent_kafka import Consumer
import threading
import time

# Basic poll loop
running = True
MIN_COMMIT_COUNT = 4

def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))

def basic_consume_loop(consumer, topics):
    global running, MIN_COMMIT_COUNT
    try:
        consumer.subscribe(topics)

        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print("Get message: key=[%s], value=[%s]" % (msg.key(), msg.value())) # Message class https://docs.confluent.io/4.1.2/clients/confluent-kafka-python/index.html#message
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(async=True) # 送出commit，更新該group.id所讀取到的offset位置，防止下次重新讀取資料
                                                # 分為四種：
                                                # 1. Basic: 不做commit動作，kafka_server自行更新offset
                                                # 2. Asynchronous: consumer.commit(async=True)，送出commit後，繼續讀取topic資訊
                                                # 3. Synchronous(at least once): consumer.commit(async=False)，送出commit後，等待回應再繼續讀取topic資訊
                                                # 4. Delivery guarantees(at most once): 處理數據前就發送commit，若數據有問題則無法在讀取，須謹慎使用
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    global running
    running = False


if __name__ == "__main__":
    
    conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "foo", # 強制參數，自訂即可,新的id會重頭讀取topic數據
        'auto.offset.reset': 'smallest',
        'on_commit': commit_completed}

    consumer = Consumer(conf)
    consumer_thread = threading.Thread(target=basic_consume_loop, args=(consumer, ["example_topic1"],))
    consumer_thread.start()
    time.sleep(30)
    shutdown()