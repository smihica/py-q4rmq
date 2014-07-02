# -*- coding: utf-8 -*-
from distutils.core import setup, Extension

setup(
    name = 'py-q4rmq',
    py_modules = ['q4rmq'],
    version = '0.0.1',
    description = 'A simple, fast, scalable and SCHEDULABLE message queue using RabbitMQ in Python.',
    author='Shin Aoyama',
    author_email = "smihica@gmail.com",
    url = "https://github.com/smihica/py-q4rmq",
    download_url = "",
    keywords = ["db", "queue", "rabbitmq"],
    install_requires = ['python-dateutil', 'amqp'],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
    long_description = """\
A simple, fast, scalable and SCHEDULABLE message queue using RabbitMQ in Python.
""",
    )
