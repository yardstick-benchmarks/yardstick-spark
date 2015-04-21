#!/usr/bin/env bash

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

# This script computes Spark's classpath and prints it to stdout; it's used by both the "run"
# script and the ExecutorRunner in standalone cluster mode.

# Figure out where Spark is installed
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

function appendToClasspath(){
  if [ -n "$1" ]; then
    if [ -n "$CLASSPATH" ]; then
      CLASSPATH="$CLASSPATH:$1"
    else
      CLASSPATH="$1"
    fi
  fi
}

if [ -n "$JAVA_HOME" ]; then
  JAR_CMD="$JAVA_HOME/bin/jar"
else
  JAR_CMD="jar"
fi

num_jars=0

for f in "${FWDIR}"/libs/*.jar; do
  appendToClasspath "$f"
done

echo "$CLASSPATH"
