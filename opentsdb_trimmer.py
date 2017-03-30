#! /usr/bin/env python2

import happybase
import logging
import subprocess
import sys

from pythonjsonlogger import jsonlogger
from struct import pack

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MIN_DELETE_AGE='1 week ago'

def setup_logging(level=logging.INFO):
    global logger
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s', datefmt="%Y-%m-%dT%H:%M:%S%z")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)

def parse_ts(args):
    if len(args) != 2:
        logger.error("Please pass a unix epoch timestamp as the only argument.")
        sys.exit(1)

    ts = int(args[1])
    if ts <= 0:
        logger.error("Please pass a valid unix epoch timestamp as the only argument.")
        sys.exit(1)

    earliest_ts = subprocess.check_output(['date', '-d', MIN_DELETE_AGE, '+%s'])
    if ts > int(earliest_ts.strip()):
        logger.error("Will not delete data newer than {}".format(MIN_DELETE_AGE))
        sys.exit(1)
    return ts

def main():
    setup_logging()
    hbase_conn = None
    ts_begin_str = pack('>I', 0)
    ts_end_str = pack('>I', parse_ts(sys.argv))

    try:
        hbase_conn = happybase.Connection("localhost", timeout=120000)
        hbase_conn.open()
    except Exception:
        logger.exception("Unable to connect to local thrift server")
        sys.exit(1)

    uid_table = hbase_conn.table('tsdb-uid')
    metrics_table = hbase_conn.table('tsdb')

    for metric,uid in uid_table.scan(columns=[b'id:metrics']):
        logger.info("Processing metric {}".format(metric))

        row_prefix_start = uid['id:metrics'] + ts_begin_str
        row_prefix_end = uid['id:metrics'] + ts_end_str

        try:
            with metrics_table.batch(batch_size=4096) as mtb:
                for ts_row_key,_ in metrics_table.scan(row_start=row_prefix_start,
                                                       row_stop=row_prefix_end):
                    mtb.delete(ts_row_key)
        except Exception:
            logger.exception("Unable to batch delete data starting around {} ".format(metric))

if __name__ == '__main__':
    main()
