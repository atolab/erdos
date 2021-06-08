"""Sends messages as fast as possible to test throughput of the DataFlow
"""

import erdos
import time
import sys
import threading
import copy
import argparse
import zenoh
import pickle
from zenoh.net import config, SubInfo, Reliability, SubMode


class ZenohRecvOp(erdos.Operator):
    def __init__(self, write_stream, size=8):
        self.write_stream = write_stream
        self.session = zenoh.net.open({})
        self.size = size


    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):

        def listener(sample):
            msg = pickle.loads(sample.payload)
            #json.loads(sample.payload.decode("utf8"))
            #msg = erdos.Message(erdos.Timestamp(coordinates=d['ts']['coordinates'], is_top=d['ts']['is_top']), d['data'])
            #print(msg)
            self.write_stream.send(msg)

        # Creating subscriber info
        sub_info = SubInfo(Reliability.Reliable, SubMode.Push)
        # declaring subscriber
        sub = self.session.declare_subscriber(f'/thr/test/{self.size}', sub_info, listener)
        while True:
            time.sleep(60)


class CallbackOp(erdos.Operator):
    def __init__(self, read_stream, scenario, name, transport, size=8):
        read_stream.add_callback(self.callback)
        self.counter = 0
        self.size = size
        self.name = name
        self.scenario = scenario
        self.transport = transport
        thread = threading.Thread(target=self.printer)
        thread.start()


    def callback(self,msg):
        #print("CallbackOp: received {msg}".format(msg=msg))
        self.counter += 1

    def printer(self):
        while True:
            time.sleep(1)
            c = copy.deepcopy(self.counter)
            self.counter = 0
            if c > 0:
                print(f"erdos-{self.transport},{self.scenario},throughput,{self.name},{self.size},{c}")


    @staticmethod
    def connect(read_streams):
        return []






def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--payload', default=8, type=int)
    parser.add_argument('-s', '--scenario', default="dataflow")
    parser.add_argument('-n', '--name', default="test")
    parser.add_argument('-t', '--transport', default="tcp")
    parser.add_argument('-g', '--graph-file')
    parser.add_argument('-o', '--operators', action='store_true')
    args = parser.parse_args()

    # creating Zenoh.net session
    #session = zenoh.net.open({})

    """Creates and runs the dataflow graph."""

    if args.operators:
        (count_stream, ) = erdos.connect(ZenohRecvOp, erdos.OperatorConfig(), [], size=args.payload)
        erdos.connect(CallbackOp, erdos.OperatorConfig(), [count_stream], size=args.payload, scenario=args.scenario, name=args.name, transport=args.transport)
        if args.graph_file is not None:
            erdos.run(args.graph_file)
        else:
            erdos.run()
    else:
        #creating Zenoh.net session
        session = zenoh.net.open({})

        ingest_stream = erdos.IngestStream()

        erdos.connect(CallbackOp, erdos.OperatorConfig(), [ingest_stream], size=args.payload, scenario=args.scenario, name=args.name, transport=args.transport)

        if args.graph_file is not None:
            erdos.run_async(args.graph_file)
        else:
            erdos.run_async()

        def listener(sample):
            msg = pickle.loads(sample.payload)
            ingest_stream.send(msg)

        # Creating subscriber info
        sub_info = SubInfo(Reliability.Reliable, SubMode.Push)
        # declaring subscriber
        sub = session.declare_subscriber(f'/thr/test/{args.payload}', sub_info, listener)

        while True:
           time.sleep(60)


if __name__ == "__main__":
    main()
