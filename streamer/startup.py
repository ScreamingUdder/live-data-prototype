from threading import Thread
from event_generator import EventGenerator


def start_streamer_daemon_threads(base_generator, parameter_controller, kafka_broker=None):
    event_generator = EventGenerator(base_generator)
    event_generator_thread = Thread(target=event_generator.run)
    event_generator_thread.daemon = True
    event_generator_thread.start()
    
    if kafka_broker is None:
	# Default to zmq
	    from fake_streamer import FakeEventStreamer
	    streamer = FakeEventStreamer(event_generator)
    else:
        from fake_streamer_kafka import FakeEventStreamer
	    streamer = FakeEventStreamer(event_generator, kafka_broker)
    
    streamer_thread = Thread(target=streamer.run)
    streamer_thread.daemon = True
    streamer_thread.start()

    parameter_controller.add_controllee(base_generator)
    parameter_controller.add_controllee(event_generator)
    parameter_controller_thread = Thread(target=parameter_controller.run)
    parameter_controller_thread.daemon = True
    parameter_controller_thread.start()
