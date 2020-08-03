import pytest
import erdos

from collections import deque


class InputGenOp(erdos.Operator):
    def __init__(self, output_stream):
        self._output_stream = output_stream
        #print("The name of the stream: {}, the ID: {}".format(
        #    output_stream._name, output_stream._id))

    @staticmethod
    def connect():
        output_stream = erdos.WriteStream()
        return [
            output_stream,
        ]

    def run(self):
        # Send 10 messages with numbers from 0 to 9.
        for i in range(10):
            timestamp = erdos.Timestamp(coordinates=[i])
            self._output_stream.send(erdos.Message(timestamp, i))
            self._output_stream.send(erdos.WatermarkMessage(timestamp))


class MapOperator(erdos.Operator):
    def __init__(self, input_stream, output_stream):
        #print("The {} stream has the ID: {}".format(input_stream._name,
        #                                            input_stream._id))
        self._message_queue = deque()
        input_stream.add_callback(self.save_messages)
        erdos.add_watermark_callback([input_stream], [output_stream],
                                     self._map_data)

    @staticmethod
    def connect(input_stream):
        output_stream = erdos.WriteStream()
        return [
            output_stream,
        ]

    def save_messages(self, message):
        #print(message)
        self._message_queue.append(message)

    def _map_data(self, timestamp, output_stream):
        retrieved_msg = self._message_queue.popleft()
        assert retrieved_msg.timestamp == timestamp, "The timestamps mismatched."
        output_stream.send(erdos.Message(timestamp, retrieved_msg.data**2))


def test_input_receiver_map():
    """ Test that ensures that a MapOperator works correctly in Python. """
    input_config = erdos.OperatorConfig(name="input_operator",
                                        flow_watermarks=False)
    map_config = erdos.OperatorConfig(name="map_operator",
                                      flow_watermarks=True)

    ingest_stream = erdos.IngestStream(_name="TEST STREAM")
    input_stream = erdos.connect(InputGenOp, input_config, [])
    [
        mapped_stream,
    ] = erdos.connect(MapOperator, map_config, input_stream)
    mapped_stream = erdos.ExtractStream(mapped_stream, _name="EXTRACT_STREAM")

    erdos.run_async()
    for i in range(10):
        output_msg = mapped_stream.read()
        print("OUTPUT: {}".format(output_msg))
        assert output_msg.data == i**2, "The output was not correct."


if __name__ == "__main__":
    test_input_receiver_map()
