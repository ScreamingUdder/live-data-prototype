from kafka import KafkaProducer
from time import sleep
import json
from flat_buf.flat_buffers_utils import FlatBuffersUtils

from logger import log


class FakeEventStreamer(object):
    def __init__(self, event_generator, kafka_broker, version=1):
        self.version = version
        self.event_generator = event_generator
        self.kafka_broker = kafka_broker
        self.producer = None

    def connect(self):
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker)

    def _send_meta_data(self, meta_data):
        header = json.dumps(self._create_meta_data_header()).encode('utf-8')
        data = json.dumps(meta_data).encode('utf-8')
        # We only send the data because we cannot combine the header and the data
        # in one message because it is not valid JSON
        self.producer.send('live_data_fake_stream', data)

    def _send_event_data(self, event_data):
        # Only send the data, not the header
        # The current flatbuffer structure we are using does not have
        # space for metadata
        buf = FlatBuffersUtils.encode_event_data(event_data)
        self.producer.send('live_data_fake_stream', bytes(buf))

    def _create_basic_header(self, packet_type):
        header = {
            'version': self.version,
            'type': packet_type,
        }
        return header

    def _create_event_data_header(self):
        header = self._create_basic_header('event_data')
        header['record_type'] = self.event_generator.get_type_info()
        return header

    def _create_meta_data_header(self):
        header = self._create_basic_header('meta_data')
        return header

    def run(self):
        log.info('Starting FakeEventStreamer (Kafka)...')
        self.connect()

        while True:
            # we first send all meta data for a pulse, then all event data
            self._send_meta_data(self.event_generator.get_meta_data())
            self._send_event_data(self.event_generator.get_events())
            sleep(1)
