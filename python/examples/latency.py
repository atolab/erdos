"""Every interveal, sends the message count to 1 receivers, the receiver
sends back the message.
This is used to compute RTT between operators.
"""
import erdos
import time
import sys
import threading
import copy
import argparse
import datetime
from erdos.operators import map


class PongOp(erdos.Operator):
    def __init__(self, read_stream, write_stream):
        read_stream.add_callback(self.callback)
        self.write_stream = write_stream


    def callback(self,msg):
        #print("PongOp: received {msg}".format(msg=msg))
        self.write_stream.send(msg)

    @staticmethod
    def connect(read_streams):
        return [erdos.WriteStream()]



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--payload', default=8, type=int)
    parser.add_argument('-s', '--scenario', default="dataflow")
    parser.add_argument('-n', '--name', default="test")
    parser.add_argument('-i', '--interveal', default=1, type=int)
    parser.add_argument('-t', '--transport', default="tcp")
    parser.add_argument('-g', '--graph-file')

    args = parser.parse_args()

    """Creates and runs the dataflow graph."""
    ingest_stream = erdos.IngestStream()
    (pong_stream, ) = erdos.connect(PongOp, erdos.OperatorConfig(), [ingest_stream])
    extract_stream = erdos.ExtractStream(pong_stream)

    if args.graph_file is not None:
        erdos.run_async(args.graph_file)
    else:
        erdos.run_async()

    count = 0
    payload = '0'*args.payload

    while True:
        msg = erdos.Message(erdos.Timestamp(coordinates=[count]), payload)
        t0 = datetime.datetime.now()
        ingest_stream.send(msg)
        #print("PingOp: sending {msg}".format(msg=msg))
        count += 1
        data = extract_stream.read()
        t1 = datetime.datetime.now()
        #print("PingOp: received {msg}".format(msg=data))
        t = t1 - t0
        print(f"erdos-{args.transport},{args.scenario},latency,{args.name},{args.payload},{data.timestamp.coordinates[0]},{t.microseconds}")
        time.sleep(args.interveal)



if __name__ == "__main__":
    main()
