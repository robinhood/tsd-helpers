import socket
import string
import struct
import sys

# adapted from https://github.com/armon/statsite/blob/master/sinks/binary_sink.py

# tcollector UDP listener
HOST = "127.0.0.1"
PORT = 8953
MY_NAME = socket.gethostname()

# Line format. We have:
# 8 byte unsigned timestamp
# 1 byte metric type
# 1 byte value type
# 2 byte key length
# 8 byte value
LINE = struct.Struct("<QBBHd")
COUNTER = struct.Struct("<I")
PREFIX_SIZE = 20

TYPE_MAP = {
  1: "kv",
  2: "counter",
  3: "timer",
  4: "set",
  5: "gauge"
}
VAL_TYPE_MAP = {
  0: "kv",
  1: "sum",
  2: "sum_sq",
  3: "mean",
  4: "count",
  5: "stddev",
  6: "min",
  7: "max",
  8: "hist_min",
  9: "hist_bin",
  10: "hist_max",
  11: "count_rate",
  12: "sample_rate",
  128: "percentile"
}
# Pre-compute all the possible percentiles
for x in xrange(1, 100):
  VAL_TYPE_MAP[128 | x] = "P%02d" % x


def main():
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  metric_template = "{metric} {ts} {value} type={type} value_type={value_type}"
  sock.connect((HOST, PORT))

  while True:
    # Read the prefix
    prefix = sys.stdin.read(20)
    if not prefix or len(prefix) != 20:
      return

    # Unpack the line
    (ts, type, val_type, key_len, val) = LINE.unpack(prefix)
    type = TYPE_MAP[type]
    val_type = VAL_TYPE_MAP[val_type]

    # Read the key. The last character in the binary key is a
    # non-printable character that messes with opentsdb input
    # sanitization. We strip that last character and use the remaining
    # as the metric name.
    key = sys.stdin.read(key_len)[0:-1]
    # Key names sometimes contain quotes, strip those away
    key = string.strip(key, '"')
    # Key names sometimes contain hostnames as a part of the key. We
    # already tack on the hostname in tcollector, so this is redundant
    # and simply expands the keyspace with not much added
    # benefit. Strip that away as well.
    key = string.replace(key, MY_NAME, "self")

    count = None
    metric_string = metric_template.format(metric=key, ts=ts, value=val,
                                           type=type, value_type=val_type)
    if val_type.startswith("hist"):
      count = COUNTER.unpack(sys.stdin.read(4))
      metric_string += " count=%d" % count

    sock.send(metric_string + "\n")


if __name__ == "__main__":
  main()

