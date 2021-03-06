# please install python if it is not present in the system
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
 name='flask-kafka',
 version='0.0.3.2',
 packages=['flask_kafka'],
 install_requires=['confluent-kafka'],
 license = 'MIT',
 description = 'An easy to use kafka consumer that uses the kafka-python library, it runs concurently with your flask server (fork to use confluent library)',
 author = 'Nimrod Kevin Maina',
 author_email = 'nimzy.maina@gmail.com',
 keywords = ['kafka','consumer','kafkaesque','flask','simple','consumer', 'flask style', 'decorator'],
 long_description=long_description,
 long_description_content_type="text/markdown",
 url="https://github.com/andydavidson/flask_kafka",
 include_package_data=True,
)