# retico
from retico_core import *

# zeromq & supporting libraries
import zmq, json
import threading
import datetime
import time
from collections import deque

class ReaderSingleton:
    __instance = None
    @staticmethod 
    def getInstance():
        """ Static access method. """
        return ReaderSingleton.__instance
    def __init__(self, ip, port):
        """ Virtually private constructor. """
        if ReaderSingleton.__instance == None:
            self.socket = zmq.Context().socket(zmq.SUB)
            self.socket.connect("tcp://{}:{}".format(ip, port))
            ReaderSingleton.__instance = self

class WriterSingleton:
    __instance = None
    @staticmethod 
    def getInstance():
        """ Static access method. """
        return WriterSingleton.__instance
    def __init__(self, ip, port):
        """ Virtually private constructor. """
        if WriterSingleton.__instance == None:
            context = zmq.Context()
            self.socket = context.socket(zmq.PUB)
            self.socket.bind("tcp://{}:{}".format(ip, port))
            WriterSingleton.__instance = self

class ZeroMQIU(abstract.IncrementalUnit):

    @staticmethod
    def type():
        return "ZeroMQ Incremental Unit"

    def __init__(self, creator=None, iuid=0, previous_iu=None, grounded_in=None,
                 payload=None, **kwargs):
        """Initialize the DialogueActIU with act and concepts.

        Args:
            act (string): A representation of the act.
            concepts (dict): A representation of the concepts as a dictionary.
        """
        super().__init__(creator=creator, iuid=iuid, previous_iu=previous_iu,
                         grounded_in=grounded_in, payload=payload)

    def set_payload(self, payload):
        self.payload = payload
        
class ZeroMQReader(abstract.AbstractProducingModule):

    """A ZeroMQ Reader Module

    Attributes:
        
    """
    @staticmethod
    def name():
        return "ZeroMQ Reader Module"

    @staticmethod
    def description():
        return "A Module providing reading from a ZeroMQ bus"

    @staticmethod
    def output_iu():
        return ZeroMQIU 

    def __init__(self, topic,  **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.
            
        """
        super().__init__(**kwargs)
        self.topic = topic
        self.reader = None

    def process_update(self, input_iu):
        '''
        This assumes that the message is json formatted, then packages it as payload into an IU
        '''
        [topic, message] = self.reader.recv_multipart()
        j = json.loads(message)
        output_iu = self.create_iu()
        if 'image' in j:
            '''
            convert image types to an imagearray as part of the payload
            '''
            payload = {}
            payload['image'] = Image.fromarray(np.array(j['image'], dtype='uint8'))
            payload['nframes'] = j['nframes']
            payload['rate'] = j['rate']
            output_iu.set_payload(payload)
        else:
            output_iu.set_payload(j)
        
        update_message = UpdateMessage()

        if "update_type" not in j:
            print("Incoming IU has no update_type!")
        if j["update_type"] == "UpdateType.ADD":
            update_message.add_iu(output_iu, abstract.UpdateType.ADD)
        elif j["update_type"] == "UpdateType.REVOKE":
            update_message.add_iu(output_iu, abstract.UpdateType.REVOKE)
        elif j["update_type"] == "UpdateType.COMMIT":
            update_message.add_iu(output_iu, abstract.UpdateType.COMMIT)
        
        return update_message


    def prepare_run(self):
        self.reader = ReaderSingleton.getInstance().socket
        self.reader.setsockopt(zmq.SUBSCRIBE, self.topic.encode())

    def setup(self):
        pass
        
class ZeroMQWriter(abstract.AbstractModule):

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
        return [abstract.IncrementalUnit] 

    def __init__(self, topic, **kwargs):
        """Initializes the ZeroMQReader.

        Args: topic(str): the topic/scope where the information will be read.
            
        """
        super().__init__(**kwargs)
        self.topic = topic.encode()
        self.queue = deque() # no maxlen
        self.writer = None
        

    def process_update(self, update_message):
        '''
        This assumes that the message is json formatted, then packages it as payload into an IU
        '''
        for um in update_message:
            self.queue.append(um)

        return None

    def run_writer(self):

        while True:
            if len(self.queue) == 0:
                time.sleep(0.1)
                continue
            input_iu, ut = self.queue.popleft()
            payload = {}
            payload['originatingTime'] = datetime.datetime.now().isoformat()
            
            # print(input_iu.payload)
            # if isinstance(input_iu, ImageIU) or isinstance(input_iu, DetectedObjectsIU)  or isinstance(input_iu, ObjectFeaturesIU):
                # payload['message'] = json.dumps(input_iu.get_json())
            # else:
            payload['message'] = json.dumps(input_iu.payload)
            payload['update_type'] = str(ut)

            self.writer.send_multipart([self.topic, json.dumps(payload).encode('utf-8')])

    def setup(self):
        self.writer = WriterSingleton.getInstance().socket
        t = threading.Thread(target=self.run_writer)
        t.start()