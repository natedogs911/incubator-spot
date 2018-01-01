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


import subprocess
import sys


class Kerberos(object):
    def __init__(self, kinit, keytab, user, kinitopts, **kwargs):

        self._kinit = kinit
        self._kinitopts = kinitopts

        self._keytab = keytab
        self._krb_user = user

        if self._kinit == None or self._kinitopts == None or self._keytab == None or self._krb_user == None:
            print("Please verify kerberos configuration, some environment variables are missing.")
            sys.exit(1)

        self._kinit_args = [self._kinit, self._kinitopts, self._keytab, self._krb_user]

        # self.authenticate(self._kinit_args)

    def authenticate(self):

        kinit = subprocess.Popen(self._kinit_args, stderr=subprocess.PIPE)
        output, error = kinit.communicate()
        if not kinit.returncode == 0:
            if error:
                print(error.rstrip())
                sys.exit(kinit.returncode)
        print("Successfully authenticated!")