import time
import numpy
import zmq
from mpi4py import MPI
import json

from logger import log
from backend_worker import BackendWorker
from kafka import KafkaConsumer, TopicPartition
from flat_buf.flat_buffers_utils import FlatBuffersUtils


# master connectes to main event stream and distributes it to all
# packets are put in a queue
class BackendEventListenerKafka(BackendWorker):
    def __init__(self, data_queue_out, kafka_broker):
        BackendWorker.__init__(self)
	self._data_queue_out = data_queue_out
	self.kafka_broker = kafka_broker
	self.consumer = None
	self.topic = None
	self.offset = -1

    def _startup(self):
        log.info('Starting EventListener...')
        self._connect()

    def _can_process_data(self):
        # TODO: for now we always say yes, i.e., we constantly wait for the stream
        return True

    def _process_data(self):
        # TODO: With this implementation the EventListener will not
        # react to commands unless stream data keeps coming in.
        what, data = self._receive_packet()
	if data is not None:
            split_data = self._distribute_stream(what, data)
	    self._data_queue_out.put(split_data)
        return True

    def _connect(self):
        if self._comm.Get_rank() == 0:
            self.consumer = KafkaConsumer(bootstrap_servers=self.kafka_broker)
	    self.topic = TopicPartition('live_data_fake_stream', 0)
	    self.consumer.assign([self.topic])
	    # For now we let it start consuming from the end
	    self.consumer.seek_to_end(self.topic)
	    self.offset = self.consumer.position(self.topic)

    def _receive_packet(self):
        if self._comm.Get_rank() == 0:
	    # Check for new data
	    # A bit hacky
            # Get highest offset
	    self.consumer.seek_to_end(self.topic)
	    end_pos = self.consumer.position(self.topic)
	    if end_pos > self.offset:
		# New data
	    	# Go back to where we were and read
            	self.consumer.seek(self.topic, self.offset)
		buf = self.consumer.next().value
		self.offset = self.consumer.position(self.topic)
		try:
		    # Simple check for JSON metadata:
		    if buf.startswith('{'):
		        # It is meta data
		        return 'meta_data', buf
	            else:
		        # Is data then
		        data = FlatBuffersUtils.decode_event_data(buf)
	                return 'event_data', data
		except Exception as err:
		    print err
        return None, None

    def _distribute_stream(self, what, data):
        if self._comm.Get_rank() == 0:
            if what == 'meta_data':
                split = [data] * self._comm.size
            else:
                split = []
                for i in range(self._comm.size):
                    split.append([])
                for i in data:
                    detector_id = int(i[0])
                    target = detector_id % self._comm.size
                    split[target].append(i)
        else:
            split = None
        what = self._comm.scatter([what]*self._comm.size, root=0)
        data = self._comm.scatter(split, root=0)
        if what == 'meta_data':
            return what, data
        else:
            return what, numpy.array(data)
