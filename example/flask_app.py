from flask import Flask
from threading import Event
import signal

from flask_kafka import FlaskKafka
app = Flask(__name__)

INTERRUPT_EVENT = Event()

kafka_params = {
    "bootstrap.servers": ",".join(["localhost:9092"]),
    "group.id": "consumer-grp-id"
}

bus = FlaskKafka(INTERRUPT_EVENT,
                 **kafka_params
                 )

# Register termination listener
def listen_kill_server():
    signal.signal(signal.SIGTERM, bus.interrupted_process)
    signal.signal(signal.SIGINT, bus.interrupted_process)
    signal.signal(signal.SIGQUIT, bus.interrupted_process)
    signal.signal(signal.SIGHUP, bus.interrupted_process)


# Handle message received from a Kafka topic
@bus.handle('test-topic')
def test_topic_handler(msg):
    print("consumed {} from test-topic".format(msg))


if __name__ == '__main__':
    # Start consuming from the Kafka server
    bus.run()
    # Termination listener
    listen_kill_server()
    # Start Flask server
    app.run(debug=True, port=5004)
