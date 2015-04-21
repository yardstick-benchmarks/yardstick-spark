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

import org.yardstickframework.*;

import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Standalone spark node.
 */
public class SparkNode implements BenchmarkServer {
    /** Master url provider. */
    private S3MasterUrlProvider masterUrlProvider;

    /** Master node. */
    private SparkMaster master;

    /** Worker node. */
    private SparkWorker worker;

    /** Url to master node. */
    private String masterUrl;

    /** {@inheritDoc} */
    @Override public void start(BenchmarkConfiguration cfg) throws Exception {
        start(cfg, true);
    }

    /**
     * @param cfg Benchmark configuration.
     * @param wait Wait termination worker.
     * @throws Exception If failed.
     */
    public void start(BenchmarkConfiguration cfg, boolean wait) throws Exception {
        masterUrlProvider = new S3MasterUrlProvider();

        setLocalIpEnv();

        masterUrl = resolveMasterUrl();

        worker = new SparkWorker();

        println(cfg, "Starting worker node.");

        worker.start(masterUrl);

        println(cfg, "Worker started.");

        if (wait)
            worker.waitTerminate();
    }

    /**
     * Init local ip for spark node.
     */
    private void setLocalIpEnv() {
        String localIp = System.getenv("LOCAL_IP");

        if (localIp != null && !localIp.isEmpty())
            System.getenv().put("SPARK_LOCAL_IP", localIp);
    }

    private String resolveMasterUrl() throws Exception {
        String masterUrl = masterUrlProvider.getMasterUrl();

        if (masterUrl == null) {
            // Master node isn't started.
            try {
                if (masterUrlProvider.tryLock()) {
                    masterUrl = masterUrlProvider.getMasterUrl();

                    if (masterUrl == null) {
                        System.out.println("Starting spark master node.");

                        // Start master node.
                        master = new SparkMaster();

                        masterUrl = master.start();

                        masterUrlProvider.registerMaster(masterUrl);

                        System.out.println("Master node started.");
                    }
                }
                else {
                    // Another process starting master node. Waiting.
                    do {
                        masterUrl = masterUrlProvider.getMasterUrl();

                        TimeUnit.MILLISECONDS.sleep(500L);
                    } while (masterUrl != null);
                }
            }
            finally {
                try {
                    masterUrlProvider.unLock();
                }
                catch (Exception ignore) {
                }
            }
        }

        return masterUrl;
    }

    /**
     * @return Url to master node.
     */
    public String masterUrl() {
        return masterUrl;
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        if (worker != null)
            worker.stop();

        if (master != null) {
            masterUrlProvider.unregisterMaster(master.url());
            master.stop();
        }
    }

    /** {@inheritDoc} */
    @Override public String usage() {
        return masterUrl();
    }
}
