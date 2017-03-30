# Various helper scripts in our OpenTSDB pipeline.

* kafka_dumper.py: Takes the incoming metric stream (stdin) and
   pushes the metrics into a kafka cluster. The script hard-codes the
   topic to metrics and find the brokers using the ZK ZNode passed as
   the first argument.

* tcollector_sink.py: We use statssite locally on every box to
   collect statsd metrics. We also have a tcollector running on every
   machine in our infrastructure. This helpers dumps the statsd
   metrics from statsite into the local tcollector's UDP listener.

* opentsdb_trimmer.py: In our OpenTSDB setup, we don't salt metrics
   coming in. For such setups, this script uses the HBase Thrift
   interface to clean up metrics older than some (configurable)
   timestamp. This timestamp is passed as an unix epoch time to the
   trimmer.
