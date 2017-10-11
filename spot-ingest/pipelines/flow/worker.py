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

import sys
import subprocess
import datetime
import logging
import os
import json
from impala.dbapi import connect
from multiprocessing import Process
from common.utils import Util


class Worker(object):

    def __init__(self,db_name,hdfs_app_path,kafka_consumer,conf_type,processes=None):
        self._initialize_members(db_name,hdfs_app_path,kafka_consumer,conf_type)

    def _initialize_members(self,db_name,hdfs_app_path,kafka_consumer,conf_type):

        # get logger instance.
        self._logger = Util.get_logger('SPOT.INGEST.WRK.FLOW')

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

        # TODO: Init impyla connection
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

        flow_date = file_name.split('.')[1]
        flow_year = flow_date[0:4]
        flow_month = flow_date[4:6]
        flow_day = flow_date[6:8]
        flow_hour = flow_date[8:10]

        # build process cmd.
        process_cmd = "nfdump -o csv -r {0}{1} {2} > {0}{1}.csv".format(self._local_staging,file_name,self._process_opt)
        self._logger.info("Processing file: {0}".format(process_cmd))
        Util.execute_cmd(process_cmd,self._logger)

        # create hdfs staging.
        hdfs_path = "{0}/flow".format(self._hdfs_app_path)
        staging_timestamp = datetime.datetime.now().strftime('%M%S%f')[:-4]
        hdfs_staging_path =  "{0}/stage/{1}".format(hdfs_path,staging_timestamp)
        create_staging_cmd = "hadoop fs -mkdir -p {0}".format(hdfs_staging_path)
        self._logger.info("Creating staging: {0}".format(create_staging_cmd))
        Util.execute_cmd(create_staging_cmd,self._logger)

        # move to stage.
        mv_to_staging ="hadoop fs -moveFromLocal {0}{1}.csv {2}/.".format(self._local_staging,file_name,hdfs_staging_path)
        self._logger.info("Moving data to staging: {0}".format(mv_to_staging))
        subprocess.call(mv_to_staging,shell=True)

        # load with impyla
        drop_table = "DROP TABLE IF EXISTS {0}.flow_tmp".format(self._db_name)
        self._logger.info( "Dropping temp table: {0}".format(drop_table))
        self._cursor.execute(drop_table)

        create_external = """
          CREATE EXTERNAL TABLE {0}.flow_tmp (
            treceived STRING,
            tryear INT,
            trmonth INT,
            trday INT,
            trhour INT,
            trminute INT,
            trsec INT,
            tdur FLOAT,
            sip  STRING,
            dip STRING,
            sport INT,
            dport INT,
            proto STRING,
            flag STRING,
            fwd INT,
            stos INT,
            ipkt BIGINT,
            ibyt BIGINT,
            opkt BIGINT,
            obyt BIGINT,
            input INT,
            output INT,
            sas INT,
            das INT,
            dtos INT,
            dir INT,
            rip STRING
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '{1}'
            TBLPROPERTIES ('avro.schema.literal'='{
            "type":   "record"
            , "name":   "RawFlowRecord"
            , "namespace" : "com.cloudera.accelerators.flows.avro"
            , "fields": [
                {"name": "treceived",               "type":["string",   "null"]}
                ,  {"name": "tryear",                "type":["float",   "null"]}
                ,  {"name": "trmonth",               "type":["float",   "null"]}
                ,  {"name": "trday",                 "type":["float",   "null"]}
                ,  {"name": "trhour",                "type":["float",   "null"]}
                ,  {"name": "trminute",              "type":["float",   "null"]}
                ,  {"name": "trsec",                 "type":["float",   "null"]}
                ,  {"name": "tdur",                  "type":["float",   "null"]}
                ,  {"name": "sip",                  "type":["string",   "null"]}
                ,  {"name": "sport",                   "type":["int",   "null"]}
                ,  {"name": "dip",                  "type":["string",   "null"]}
                ,  {"name": "dport",                   "type":["int",   "null"]}
                ,  {"name": "proto",                "type":["string",   "null"]}
                ,  {"name": "flag",                 "type":["string",   "null"]}
                ,  {"name": "fwd",                     "type":["int",   "null"]}
                ,  {"name": "stos",                    "type":["int",   "null"]}
                ,  {"name": "ipkt",                 "type":["bigint",   "null"]}
                ,  {"name": "ibytt",                "type":["bigint",   "null"]}
                ,  {"name": "opkt",                 "type":["bigint",   "null"]}
                ,  {"name": "obyt",                 "type":["bigint",   "null"]}
                ,  {"name": "input",                   "type":["int",   "null"]}
                ,  {"name": "output",                  "type":["int",   "null"]}
                ,  {"name": "sas",                     "type":["int",   "null"]}
                ,  {"name": "das",                     "type":["int",   "null"]}
                ,  {"name": "dtos",                    "type":["int",   "null"]}
                ,  {"name": "dir",                     "type":["int",   "null"]}
                ,  {"name": "rip",                  "type":["string",   "null"]}
            ]
          }')
          """.format(self._db_name, hdfs_staging_path)
        self._logger.info( "Creating external table: {0}".format(create_external))
        self._cursor.execute(create_external)

        insert_into_table = """
        INSERT INTO TABLE {0}.flow
        PARTITION (y={1}, m={2}, d={3}, h={4})
        SELECT   treceived,  unix_timestamp(treceived) AS unix_tstamp, tryear,  trmonth, trday,  trhour,  trminute,  trsec,
          tdur,  sip, dip, sport, dport,  proto,  flag,  fwd,  stos,  ipkt,  ibyt,  opkt,  obyt,  input,  output,
          sas,  das,  dtos,  dir,  rip
        FROM {0}.flow_tmp
        """.format(self._db_name, flow_year, flow_month, flow_day, flow_hour)
        self._logger.info( "Loading data to {1}: {0}".format(insert_into_table,
                                                             self._db_name
                                                             ))
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
