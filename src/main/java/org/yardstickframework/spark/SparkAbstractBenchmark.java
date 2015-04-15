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

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Abstract class for Ignite benchmarks.
 */
public abstract class SparkAbstractBenchmark extends BenchmarkDriverAdapter {
    /** Arguments. */
    protected final SparkBenchmarkArguments args = new SparkBenchmarkArguments();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        jcommander(cfg.commandLineArguments(), args, "<ignite-driver>");
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
    }

    /** {@inheritDoc} */
    @Override public String usage() {
        return BenchmarkUtils.usage(args);
    }
}
