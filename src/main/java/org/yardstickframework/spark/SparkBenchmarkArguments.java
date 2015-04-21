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

import com.beust.jcommander.*;

/**
 * Input arguments for Ignite benchmarks.
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
public class SparkBenchmarkArguments {
    /** */
    @Parameter(names = {"-b", "--backups"}, description = "Backups")
    private boolean backups;

    /** */
    @Parameter(names = {"-r", "--range"}, description = "Key range")
    private int range = 1_000_000;

    /** */
    @Parameter(names = {"-bch", "--batchSize"}, description = "Batch size")
    private int batchSize = 1_000;

    /**
     * @return Backups.
     */
    public boolean backups() {
        return backups;
    }

    /**
     * @return Key range, from {@code 0} to this number.
     */
    public int range() {
        return range;
    }

    /**
     * @return Batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Description.
     */
    public String description() {
        return "-r=" + range + " -b=" + backups + " -bch=" + batchSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return description();
    }
}
