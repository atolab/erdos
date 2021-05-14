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
            #time.sleep(1)

class ZenohSendOp(erdos.Operator):
    def __init__(self, read_stream, size=8):
        read_stream.add_callback(self.callback)
        self.session = zenoh.net.open({})
        self.size = size
        self.rid = self.session.declare_resource(f'/thr/test/{self.size}')
        self.publisher = self.session.declare_publisher(self.rid)


    def callback(self,msg):
        #print(f"ZenohSendOp: received {msg} sending to /thr/test/{self.size}")
        data = pickle.dumps(msg)
        # ts = msg.timestamp
        # value = msg.data
        # d = json.dumps({'ts':{'coordinates':ts.coordinates, 'is_top':ts._is_top}, 'data':value})
        self.session.write(self.rid, data)
        #print(f"ZenohSendOp: sent to /thr/test/{self.size}")


    @staticmethod
    def connect(read_streams):
        return []


class Counter(object):
    def __init__(self,scenario, name, transport, size=8):
        self.counter = 0
        self.size = size
        self.name = name
        self.scenario = scenario
        self.transport = transport
        thread = threading.Thread(target=self.printer)
        thread.start()

    def printer(self):
            while True:
                time.sleep(1)
                c = copy.deepcopy(self.counter)
                self.counter = 0
                if c > 0:
                    print(f"erdos-{self.transport},{self.scenario},throughput,{self.name},{self.size},{c}")
    def inc(self):
        self.counter += 1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--payload', default=8, type=int)
    parser.add_argument('-s', '--scenario', default="dataflow")
    parser.add_argument('-n', '--name', default="test")
    parser.add_argument('-t', '--transport', default="tcp")
    parser.add_argument('-o', '--operators', action='store_true')
    parser.add_argument('-g', '--graph-file')
    args = parser.parse_args()

    zenoh.init_logger()

    # creating Zenoh.net session
    #session = zenoh.net.open({})

    """Creates and runs the dataflow graph."""
    (count_stream, ) = erdos.connect(SendOp, erdos.OperatorConfig(), [], size=args.payload)

    if args.operators:
        erdos.connect(ZenohSendOp, erdos.OperatorConfig(), [count_stream], size=args.payload)
        if args.graph_file is not None:
            erdos.run(args.graph_file)
        else:
            erdos.run()
    else:
        extract_stream = erdos.ExtractStream(count_stream)
        # creating zenoh session
        session = zenoh.net.open({})

        # Declaring zenoh sender
        rid = session.declare_resource(f'/thr/test/{args.payload}')
        publisher = session.declare_publisher(rid)

        if args.graph_file is not None:
            erdos.run_async(args.graph_file)
        else:
            erdos.run_async()

        counter = Counter(size=args.payload, scenario=args.scenario, name=args.name, transport=args.transport)
        while True:
            msg = extract_stream.read()
            counter.inc()

            #data = pickle.dumps(msg)
            #session.write(rid, data)



if __name__ == "__main__":
    main()