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
import org.apache.spark.deploy.worker.*;
import scala.*;

/**
 * Spark worker node.
 */
public class SparkWorker {
    /** Worker. */
    Tuple2<ActorSystem, Object> worker;

    /**
     * Start worker node.
     *
     * @param masterUrl Url to master. <i>spark://HOST:PORT</i>
     */
    public void start(String masterUrl) {
        WorkerArguments args = new WorkerArguments(new String[]{masterUrl}, new SparkConf());

        worker = Worker.startSystemAndActor(
            args.host(),
            args.port(),
            args.webUiPort(),
            args.cores(),
            args.memory(),
            args.masters(),
            args.workDir(),
            Option.apply((Object) 1),
            new SparkConf()
        );
    }

    /**
     * Wait termination worker.
     */
    public void waitTerminate() {
        worker._1().awaitTermination();
    }

    /**
     * Stop node.
     */
    public void stop() {
        if (worker != null)
            worker._1().shutdown();
    }
}
