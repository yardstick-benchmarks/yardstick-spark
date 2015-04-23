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

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.yardstickframework.*;

import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Abstract class for Ignite benchmarks.
 */
public abstract class SparkAbstractBenchmark extends BenchmarkDriverAdapter {
    /** Arguments. */
    protected final SparkBenchmarkArguments args = new SparkBenchmarkArguments();

    /** Spark node. */
    private SparkNode node;

    /** Spark context. */
    protected JavaSparkContext sc;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        jcommander(cfg.commandLineArguments(), args, "<spark-driver>");

        //node = new SparkNode();

        //node.start(cfg, false);
        S3MasterUrlProvider urlProvider = new S3MasterUrlProvider();

        sc = new JavaSparkContext(new SparkConf()
            .setAppName("query")
            .set("spark.akka.frameSize", "128")
            .set("spark.eventLog.enabled", "true")
            .set("spark.driver.host", System.getenv("LOCAL_IP"))
            .setMaster(urlProvider.getMasterUrl()));
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (sc != null)
            sc.stop();

        if (node != null)
            node.stop();
    }

    /** {@inheritDoc} */
    @Override public String usage() {
        return BenchmarkUtils.usage(args);
    }

    /**
     * @param max Key range.
     * @return Next key.
     */
    protected int nextRandom(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }
}
