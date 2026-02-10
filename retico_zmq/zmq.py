"""
ZeroMQ Module
=============

This module defines two incremental modules ZeroMQReader and ZeroMQWriter that act as a
a bridge between ZeroMQ and retico. For this, a ZeroMQIU is defined that contains the
information revceived over the ZeroMQ bridge.
"""
import datetime
import pickle
import threading
import time
from collections import deque

# zeromq & supporting libraries
import zmq

# retico
import retico_core


class ZMQReaderModule(retico_core.AbstractModule):

    """A ZeroMQ Reader Module

    Attributes:

    """

    def name(self):
        return f"ZMQ Reader - Forwards input IUs "

    @staticmethod
    def description():
        return "A Module providing reading from a ZeroMQ bus"

    @staticmethod
    def input_ius():
        # Input IUs are not relevant here because we receive IUs from ZMQ message queue not from the output of a module
        return []

    @staticmethod
    def output_iu():
        # output IU does not have any impact because this module does not create any IUs, just passes existing ones on.
        pass

    def __init__(self, ip, port, topic, **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.

        """
        super().__init__(**kwargs)
        self.queue = deque()
        self.target_iu_types = {}
        self.socket = zmq.Context().socket(zmq.SUB)
        self.socket.connect("tcp://{}:{}".format(ip, port))
        self.topic = topic
        self.socket.subscribe(topic)

    def process_update(self, input_iu):
        """
        As an edge case, process_update is not used for Reader Modules because updates do not come from being subscribed to another module.
        Rather, they come from populating a queue with messages from the ZMQ writer filtered by the appropriate topic
        """
        # the _run function in the Abstract module we inherit expects to be able to call process update. Return None as a no-op
        return None

    def process_message(self):
        """
        Read ZMQ message from reader queue, load input IU from pickle, and pass along
        """
        while True:
            time.sleep(0.02) # prevent tight loop if queue is empty
            if len(self.queue) > 0:
                message = self.queue.popleft()
                pickle_load_start_time = time.time()
                input_iu, update_type = pickle.loads(message)
                print(f"Took {time.time() - pickle_load_start_time} seconds to unpickle IU")
                print(f"Incoming message {input_iu.type()} is {input_iu.age()} seconds old")
                um = retico_core.UpdateMessage.from_iu(input_iu, update_type) # pass input IU on using its own update type
                self.append(um)

    def run_reader(self):
        """
        Wait for messages from the writer for the defined topic, add to queue to be processed
        ZMQ handles topic management
        """
        while True:
            topic,message = self.socket.recv_multipart()
            print(f"Receiving message over ZMQ: {datetime.datetime.now().isoformat()}")
            self.queue.append(message)

    def prepare_run(self):
        t = threading.Thread(target=self.run_reader, daemon=True)
        t.start()
        t = threading.Thread(target=self.process_message, daemon=True)
        t.start()


class WriterSingleton:
    __instance = None

    @staticmethod
    def getInstance():
        """Static access method."""
        return WriterSingleton.__instance

    def __init__(self, ip, port):
        """Virtually private constructor."""
        if WriterSingleton.__instance is None:
            context = zmq.Context()
            self.queue = deque()
            self.socket = context.socket(zmq.PUB)
            self.socket.bind("tcp://{}:{}".format(ip, port))
            WriterSingleton.__instance = self
            t = threading.Thread(target=self.run_writer)
            t.daemon = True
            t.start()

    def add_to_queue(self, data):
        self.queue.append(data)

    def run_writer(self):
        while True:
            if len(self.queue) == 0:
                time.sleep(0.1)
                continue
            topic, message, update_type = self.queue.popleft()
            serialize_start_time = time.time()
            serialized_message = pickle.dumps((message, update_type))
            print(f"Took {time.time() - serialize_start_time} seconds to pickle IU ({len(serialized_message)} bytes)")
            print(f"Sending message over ZMQ: {datetime.datetime.now().isoformat()}")
            self.socket.send_multipart([topic.encode(), serialized_message])

class ZeroMQWriter(retico_core.AbstractModule):

    """A ZeroMQ Writer Module

    Note: If you are using this to pass IU payloads to PSI, make sure you're passing JSON-formatable stuff (i.e., dicts not tuples)

    Attributes:
    topic (str): topic/scope that this writes to
    """

    @staticmethod
    def name():
        return "ZeroMQ Writer Module"

    @staticmethod
    def description():
        return "A Module providing writing onto a ZeroMQ bus"

    @staticmethod
    def output_iu():
        return None

    @staticmethod
    def input_ius():
        return [retico_core.IncrementalUnit]

    def __init__(self, topic, **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.

        """
        super().__init__(**kwargs)
        self.topic = topic
        self.queue = deque()  # no maxlen
        self.writer = WriterSingleton.getInstance()

    def process_update(self, update_message):
        """
        Adds the IU to the writer queue
        """
        for input_iu,update_type in update_message:
            self.writer.add_to_queue([self.topic, input_iu, update_type])
        return None

    def prepare_run(self):
        pass
