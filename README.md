# Various helper scripts in our OpenTSDB pipeline.

* kafka_dumper.py: Takes the incoming metric stream (stdin) and pushes
   the metrics into a kafka cluster. The script hard-codes the topic
   to metrics and find the brokers using the ZNode passed as the first
   argument.

* tcollector_sink.py: We
   use [Statsite](https://github.com/statsite/statsite) locally on
   every box to collect statsd metrics. We also have a tcollector
   running on every machine in our infrastructure. This helper dumps
   the statsd metrics from statsite into the local tcollector's UDP
   listener.

* opentsdb_trimmer.py: We currently don't salt metrics when we store
   them in HBase. For such setups, this script uses the HBase Thrift
   interface to clean up metrics older than some (configurable)
   timestamp. This timestamp is passed as an unix epoch time to the
   trimmer.
