/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yardstickframework.spark;

import akka.actor.*;
import org.apache.spark.*;
import org.apache.spark.deploy.master.*;
import scala.*;

import java.util.concurrent.TimeUnit;

/**
 * Spark master node.
 */
public class SparkMaster {
    /** */
    public static final String SPARK_URL_PREFIX = "spark:";

    /** */
    public static final int DEFAULT_PORT = 55555;

    /** */
    public static final String DEFAULT_HOST = "localhost";

    /** */
    public static final int DEFAULT_WEB_UI_PORT = 66666;

    /** */
    private Tuple4<ActorSystem, Object, Object, Option<Object>> startInfo;

    /** */
    private String url;

    /**
     * @return Url for master node.
     */
    public String start() {
        startInfo = Master.startSystemAndActor(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_WEB_UI_PORT, new SparkConf());

        url = SPARK_URL_PREFIX + "//" + DEFAULT_HOST + ":" + DEFAULT_PORT;

        return url;
    }

    /**
     * @return Url to master node.
     */
    public String url() {
        return url;
    }

    /**
     * Stop master node.
     */
    public void stop() {
        if (startInfo != null)
            startInfo._1().shutdown();
    }
}
