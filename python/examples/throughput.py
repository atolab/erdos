"""Sends messages as fast as possible to test throughput of the DataFlow
"""

import erdos
import time
import sys
import threading
import copy
import argparse


class SendOp(erdos.Operator):
    def __init__(self, write_stream, size=8):
        self.write_stream = write_stream
        self.size=size


    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = 0
        payload = '0'*self.size
        while True:
            msg = erdos.Message(erdos.Timestamp(coordinates=[count]), payload)
            self.write_stream.send(msg)
            #print("SendOp: sending {msg}".format(msg=msg))
            count += 1


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
    args = parser.parse_args()

    """Creates and runs the dataflow graph."""
    (count_stream, ) = erdos.connect(SendOp, erdos.OperatorConfig(), [], size=args.payload)
    erdos.connect(CallbackOp, erdos.OperatorConfig(), [count_stream], size=args.payload, scenario=args.scenario, name=args.name, transport=args.transport)
    if args.graph_file is not None:
        erdos.run(args.graph_file)
    else:
        erdos.run()


if __name__ == "__main__":
    main()
