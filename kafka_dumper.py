import json
import logging
import sys
import time

from kafka import KafkaClient, SimpleProducer
from kazoo.client import KazooClient


logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
kafka_producer = None


def get_kafka_brokers(kafka_znode):
    """
    Get the comma separated list of kafka broker addresses

    Kafka stores data about the location of brokers in zookeeper. This data
    is stored under the zookeeper path "/brokers/ids/broker_id/",
    where broker_id is the unique id of each broker.

    The information is stored in a json object and we are interested in the
    values associated with the keys "host" and "port".
    """
    zk_hosts = ""
    brokers_path = "/brokers/ids"
    if "/" in kafka_znode:
        zk_hosts, znode_path = kafka_znode.split("/", 1)
        brokers_path = "/" + znode_path + "/brokers/ids"
    else:
        zk_hosts = kafka_znode

    zookeeper_client = KazooClient(
        hosts=zk_hosts,
        read_only=True
    )

    zookeeper_client.start()
    kafka_brokers = zookeeper_client.get_children(brokers_path)
    kafka_hosts = []
    for broker in kafka_brokers:
        broker_data_str, broker_metadata = zookeeper_client.get(
            "{}/{}".format(brokers_path, broker)
        )
        broker_data = json.loads(broker_data_str)
        kafka_hosts.append(
            "{}:{}".format(broker_data["host"], broker_data["port"])
        )
    zookeeper_client.stop()
    return ",".join(kafka_hosts)


def make_kafka_producer(kafka_znode):
    kafka_brokers = get_kafka_brokers(kafka_znode)
    kafka_client = KafkaClient(kafka_brokers)
    return SimpleProducer(
        kafka_client,
        async=False,
        req_acks=1,
        random_start=True
    )


def send_metrics_to_kafka(msgs, kafka_znode):
    global kafka_producer, logger
    metric_index = 1
    kafka_msgs = []
    # metric lines usually look like "put proc.stat.cpu 354627253 26  host=foo"
    for line in msgs:
        metric_parts = line.split()
        kafka_msgs.append(" ".join(metric_parts[metric_index:]))
    msgs_sent = False
    while not msgs_sent:
        try:
            if kafka_producer is None:
                kafka_producer = make_kafka_producer(kafka_znode)
            kafka_producer.send_messages(
                "metrics",
                *kafka_msgs
            )
            msgs_sent = True
        except:
            logger.exception("Exception while sending metrics to kafka, re-trying in 2s")
            if kafka_producer:
                kafka_producer.stop()
            kafka_producer = None
            time.sleep(2)


def process_stdin(kafka_znode):
    batch_size = 64
    msgs = []
    for line in sys.stdin:
        # metric lines usually look like "put proc.stat.cpu 354627253 26  host=foo"
        if not line.startswith("put "):
            continue
        msgs.append(line)
        if len(msgs) < batch_size:
            continue
        send_metrics_to_kafka(msgs, kafka_znode)
        msgs = []
    if len(msgs) > 0:
        send_metrics_to_kafka(msgs, kafka_znode)


if __name__ == "__main__":
    process_stdin(sys.argv[1])

