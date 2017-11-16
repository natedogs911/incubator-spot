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
import datetime
import subprocess
import json
import os
from multiprocessing import Process
from common.utils import Util
from impala.dbapi import connect


class Worker(object):

    def __init__(self,db_name,hdfs_app_path,kafka_consumer,conf_type,processes=None):
        
        self._initialize_members(db_name,hdfs_app_path,kafka_consumer,conf_type)

    def _initialize_members(self,db_name,hdfs_app_path,kafka_consumer,conf_type):

        # get logger instance.
        self._logger = Util.get_logger('SPOT.INGEST.WRK.DNS')

        self._db_name = db_name
        self._hdfs_app_path = hdfs_app_path

        # read proxy configuration.
        self._script_path = os.path.dirname(os.path.abspath(__file__))
        conf_file = "{0}/ingest_conf.json".format(os.path.dirname(os.path.dirname(self._script_path)))
        conf = json.loads(open(conf_file).read())
        self._conf = conf["pipelines"][conf_type] 

        self._process_opt = self._conf['process_opt']
        self._local_staging = self._conf['local_staging']
        self.kafka_consumer = kafka_consumer

        self._hs2_host = os.getenv("HS2_HOST")
        self._hs2_port = os.getenv("HS2_PORT")
        self._conn = connect(
            host=self._hs2_host,
            port=int(self._hs2_port),
            auth_mechanism='GSSAPI',
            kerberos_service_name='hive',
            database=db_name
        )
        self._cursor = self._conn.cursor()

    def start(self):

        self._logger.info("Listening topic:{0}".format(self.kafka_consumer.Topic))
        for message in self.kafka_consumer.start():
            self._new_file(message.value)

    def _new_file(self,file):

        self._logger.info("-------------------------------------- New File received --------------------------------------")
        self._logger.info("File: {0} ".format(file))        
        p = Process(target=self._process_new_file, args=(file,))
        p.start() 
        p.join()

    def _process_new_file(self,file):


        # get file from hdfs
        get_file_cmd = "hadoop fs -get {0} {1}.".format(file,self._local_staging)
        self._logger.info("Getting file from hdfs: {0}".format(get_file_cmd))
        Util.execute_cmd(get_file_cmd,self._logger)

        # get file name and date
        file_name_parts = file.split('/')
        file_name = file_name_parts[len(file_name_parts)-1]

        binary_hour = file_name_parts[len(file_name_parts)-2]
        binary_date_path = file_name_parts[len(file_name_parts)-3]
        binary_year = binary_date_path[0:4]
        binary_month = binary_date_path[4:6]
        binary_day = binary_date_path[6:8]

        # build process cmd.
        process_cmd = "tshark -r {0}{1} {2} > {0}{1}.csv".format(self._local_staging,file_name,self._process_opt)
        self._logger.info("Processing file: {0}".format(process_cmd))
        Util.execute_cmd(process_cmd,self._logger)

        # create hdfs staging.
        hdfs_path = "{0}/dns".format(self._hdfs_app_path)
        staging_timestamp = datetime.datetime.now().strftime('%M%S%f')[:-4]
        hdfs_staging_path =  "{0}/stage/{1}".format(hdfs_path,staging_timestamp)
        create_staging_cmd = "hadoop fs -mkdir -p {0}".format(hdfs_staging_path)
        self._logger.info("Creating staging: {0}".format(create_staging_cmd))
        Util.execute_cmd(create_staging_cmd,self._logger)

        # move to stage.
        mv_to_staging ="hadoop fs -moveFromLocal {0}{1}.csv {2}/.".format(self._local_staging,file_name,hdfs_staging_path)
        self._logger.info("Moving data to staging: {0}".format(mv_to_staging))
        Util.execute_cmd(mv_to_staging,self._logger)

        #load to avro
        #load_to_avro_cmd = "hive -hiveconf dbname={0} -hiveconf
        # y={1} -hiveconf m={2} -hiveconf d={3} -hiveconf h={4} -hiveconf data_location='{5}'
        #  -f pipelines/dns/load_dns_avro_parquet.hql".format(self._db_name,binary_year,binary_month,binary_day,binary_hour,hdfs_staging_path)
        drop_table = 'DROP TABLE IF EXISTS {0}.dns_tmp'.format(self._db_name)
        self._cursor.execute(drop_table)

        # Create external table
        create_external = ("\n"
                           "CREATE EXTERNAL TABLE {0}.dns_tmp (\n"
                           "  frame_day STRING,\n"
                           "  frame_time STRING,\n"
                           "  unix_tstamp BIGINT,\n"
                           "  frame_len INT,\n"
                           "  ip_src STRING,\n"
                           "  ip_dst STRING,\n"
                           "  dns_qry_name STRING,\n"
                           "  dns_qry_type INT,\n"
                           "  dns_qry_class STRING,\n"
                           "  dns_qry_rcode INT,\n"
                           "  dns_a STRING  \n"
                           "  )\n"
                           "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n"
                           "  STORED AS TEXTFILE\n"
                           "  LOCATION '{1}'\n"
                           "  TBLPROPERTIES ('avro.schema.literal'='{{\n"
                           "  \"type\":   \"record\"\n"
                           "  , \"name\":   \"RawDnsRecord\"\n"
                           "  , \"namespace\" : \"com.cloudera.accelerators.dns.avro\"\n"
                           "  , \"fields\": [\n"
                           "      {{\"name\": \"frame_day\",        \"type\":[\"string\", \"null\"]}\n"
                           "      , {{\"name\": \"frame_time\",     \"type\":[\"string\", \"null\"]}\n"
                           "      , {{\"name\": \"unix_tstamp\",    \"type\":[\"bigint\", \"null\"]}\n"
                           "      , {{\"name\": \"frame_len\",      \"type\":[\"int\",    \"null\"]}\n"
                           "      , {{\"name\": \"ip_src\",         \"type\":[\"string\", \"null\"]}\n"
                           "      , {{\"name\": \"ip_dst\",         \"type\":[\"string\", \"null\"]}\n"
                           "      , {{\"name\": \"dns_qry_name\",   \"type\":[\"string\", \"null\"]}\n"
                           "      , {{\"name\": \"dns_qry_type\",   \"type\":[\"int\",    \"null\"]}\n"
                           "      , {{\"name\": \"dns_qry_class\",  \"type\":[\"string\", \"null\"]}\n"
                           "      , {{\"name\": \"dns_qry_rcode\",  \"type\":[\"int\",    \"null\"]}\n"
                           "      , {{\"name\": \"dns_a\",          \"type\":[\"string\", \"null\"]}\n"
                           "      ]\n"
                           "}')\n"
                           ).format(self._db_name, hdfs_staging_path)
        self._logger.info( "Creating external table: {0}".format(create_external))
        self._cursor.execute(create_external)

        # Insert data
        insert_into_table = """
            INSERT INTO TABLE {0}.dns
            PARTITION (y={1}, m={2}, d={3}, h={4)
            SELECT   CONCAT(frame_day , frame_time) as treceived, unix_tstamp, frame_len, ip_dst, ip_src, dns_qry_name,
            dns_qry_class,dns_qry_type, dns_qry_rcode, dns_a 
            FROM {0}.dns_tmp
        """.format(self._db_name,binary_year,binary_month,binary_day,binary_hour)
        self._logger.info( "Loading data to {0}: {1}"
                           .format(self._db_name, insert_into_table)
                           )
        self._cursor.execute(insert_into_table)

        # remove from hdfs staging
        rm_hdfs_staging_cmd = "hadoop fs -rm -R -skipTrash {0}".format(hdfs_staging_path)
        self._logger.info("Removing staging path: {0}".format(rm_hdfs_staging_cmd))
        Util.execute_cmd(rm_hdfs_staging_cmd,self._logger)

        # remove from local staging.
        rm_local_staging = "rm {0}{1}".format(self._local_staging,file_name)
        self._logger.info("Removing files from local staging: {0}".format(rm_local_staging))
        Util.execute_cmd(rm_local_staging,self._logger)

        self._logger.info("File {0} was successfully processed.".format(file_name))
