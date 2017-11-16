#!/bin/env python 

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
from common.utils import Util
from confluent_kafka import Producer
from confluent_kafka import Consumer
from kafka.common import TopicPartition

class KafkaTopic(object):


    def __init__(self,topic,server,port,zk_server,zk_port,partitions):

        self._initialize_members(topic,server,port,zk_server,zk_port,partitions)

    def _initialize_members(self,topic,server,port,zk_server,zk_port,partitions):

        # get logger isinstance
        self._logger = logging.getLogger("SPOT.INGEST.KAFKA")

        # kafka requirements
        self._server = server
        self._port = port
        self._zk_server = zk_server
        self._zk_port = zk_port
        self._topic = topic
        self._num_of_partitions = partitions
        self._partitions = []
        self._partitioner = None

        # create topic with partitions
        self._create_topic()

    @classmethod
    def producer_config(cls, server, conf):
        # type: (dict) -> dict
        """Returns a configuration dictionary containing optional values"""

        kerbconf = conf['kerberos']
        sslconf = conf['ssl']

        connection_conf = {
            'bootstrap.servers': server,
            'api.version.request.timeout.ms': 3600000
        }

        if kerbconf['enabled'] == 'true':
            connection_conf.update({
                'sasl.mechanisms': kerbconf['sasl_mech'],
                'security.protocol': kerbconf['security_proto'],
                'sasl.kerberos.principal': kerbconf['principal'],
                'sasl.kerberos.keytab': kerbconf['keytab'],
                'sasl.kerberos.min.time.before.relogin': kerbconf['min_relogin']
            })

            sn = kerbconf['service_name']
            if sn:
                connection_conf.update({'sasl.kerberos.service.name': sn})

            kinit_cmd = kerbconf['kinit']
            if kinit_cmd:
                connection_conf.update({'sasl.kerberos.kinit.cmd': kinit_cmd})

        if 'SSL' in kerbconf['security_proto']:
            connection_conf.update({
                'ssl.ca.location': sslconf['ca_location'],
                'ssl.certificate.location': sslconf['cert'],
                'ssl.key.location': sslconf['key']
            })

        return connection_conf

    def _create_topic(self):

        self._logger.info("Creating topic: {0} with {1} parititions".format(self._topic,self._num_of_partitions))
        
        # get script path 
        zk_conf = "{0}:{1}".format(self._zk_server,self._zk_port)
        create_topic_cmd = "{0}/kafka_topic.sh create {1} {2} {3}".format(os.path.dirname(os.path.abspath(__file__)),self._topic,zk_conf,self._num_of_partitions)

        # execute create topic cmd
        Util.execute_cmd(create_topic_cmd,self._logger)

    
    @classmethod
    def SendMessage(cls,message,topic,kafka_conf):
        p = Producer(**kafka_conf)
        future = p.produce(topic,message.encode('utf-8'))
        p.flush(timeout=3600000)

    @property
    def Topic(self):
        return self._topic
    
    @property
    def Partition(self):        
        return self._partitioner.partition(self._topic).partition

    @property
    def Zookeeper(self):
        zk = "{0}:{1}".format(self._zk_server,self._zk_port)
        return zk

    @property
    def BootstrapServers(self):
        servers = "{0}:{1}".format(self._server,self._port) 
        return servers


class KafkaConsumer(object):
    
    def __init__(self,topic,server,port,zk_server,zk_port,partition):

        self._initialize_members(topic,server,port,zk_server,zk_port,partition)

    def _initialize_members(self,topic,server,port,zk_server,zk_port,partition):

        self._topic = topic
        self._server = server
        self._port = port
        self._zk_server = zk_server
        self._zk_port = zk_port
        self._id = partition

    @classmethod
    def consumer_config(cls, id, server, conf):
        # type: (dict) -> dict
        """Returns a configuration dictionary containing optional values"""

        kerbconf = conf['kerberos']
        sslconf = conf['ssl']

        connection_conf = {
            'bootstrap.servers': server,
            'group.id': id,
            'api.version.request.timeout.ms': 3600000
        }

        if kerbconf['enabled'] == 'true':
            connection_conf.update({
                'sasl.mechanisms': kerbconf['sasl_mech'],
                'security.protocol': kerbconf['security_proto'],
                'sasl.kerberos.principal': kerbconf['principal'],
                'sasl.kerberos.keytab': kerbconf['keytab'],
                'sasl.kerberos.min.time.before.relogin': kerbconf['min_relogin']
            })

            sn = kerbconf['service_name']
            if sn:
                connection_conf.update({'sasl.kerberos.service.name': sn})

            kinit_cmd = kerbconf['kinit']
            if kinit_cmd:
                connection_conf.update({'sasl.kerberos.kinit.cmd': kinit_cmd})

        if 'SSL' in kerbconf['security_proto']:
            connection_conf.update({
                'ssl.certificate.location': sslconf['ca_cert'],
                'ssl.ca.location': sslconf['ca_location'],
                'ssl.key.location': sslconf['key']
            })

        return connection_conf

    def start(self, kafka_conf):
        
        kafka_brokers = '{0}:{1}'.format(self._server,self._port)
        consumer = Consumer(**kafka_conf)
        consumer.subscribe(self._topic)
        return consumer

    @property
    def Topic(self):
        return self._topic

    @property
    def ZookeperServer(self):
        return "{0}:{1}".format(self._zk_server,self._zk_port)

    
