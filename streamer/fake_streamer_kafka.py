from kafka import KafkaProducer
from time import sleep
import json

from logger import log


class FakeEventStreamer(object):
    def __init__(self, eventGenerator, kafka_broker, version=1):
        self.version = version
        self.eventGenerator = eventGenerator
	self.kafka_broker = kafka_broker
	self.producer = None

    def connect(self):
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker)

    def _send_meta_data(self, meta_data):
        header = json.dumps(self._create_meta_data_header()).encode('utf-8')
	
        self.producer.send('live_data_fake_stream', header + meta_data)

    def _send_event_data(self, event_data):
        header = json.dumps(self._create_event_data_header()).encode('utf-8')
        self.producer.send('live_data_fake_stream', header + event_data)

    def _create_basic_header(self, packet_type):
        header = {
                'version':self.version,
                'type':packet_type,
                }
        return header

    def _create_event_data_header(self):
        header = self._create_basic_header('event_data')
        header['record_type'] = self.eventGenerator.get_type_info()
        return header

    def _create_meta_data_header(self):
        header = self._create_basic_header('meta_data')
        return header

    def run(self):
        log.info('Starting FakeEventStreamer (Kafka)...')
        self.connect()

        while True:
            # we first send all meta data for a pulse, then all event data
            self._send_meta_data(self.eventGenerator.get_meta_data())
            self._send_event_data(self.eventGenerator.get_events())
	    sleep(1)
