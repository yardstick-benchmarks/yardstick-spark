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
    private int backups;

    @Parameter(names = {"-cfg", "--Config"}, description = "Configuration file")
    private String cfg = "config/ignite-localhost-config.xml";

    /** */
    @Parameter(names = {"-r", "--range"}, description = "Key range")
    private int range = 1_000_000;

    /** */
    @Parameter(names = {"-j", "--jobs"}, description = "Number of jobs for compute benchmarks")
    private int jobs = 10;

    /** */
    @Parameter(names = {"-cs", "--cacheStore"}, description = "Enable or disable cache store readThrough, writeThrough")
    private boolean storeEnabled;

    /** */
    @Parameter(names = {"-wb", "--writeBehind"}, description = "Enable or disable writeBehind for cache store")
    private boolean writeBehind;

    /** */
    @Parameter(names = {"-cn", "--cacheName"}, description = "Cache name")
    private String cacheName;

    /** */
    @Parameter(names = {"-bch", "--batchSize"}, description = "Batch size")
    private int batchSize = 1_000;

    /**
     * @return Backups.
     */
    public int backups() {
        return backups;
    }

    /**
     * @return Key range, from {@code 0} to this number.
     */
    public int range() {
        return range;
    }

    /**
     * @return Configuration file.
     */
    public String configuration() {
        return cfg;
    }

    /**
     * @return Number of jobs
     */
    public int jobs() {
        return jobs;
    }

    /**
     * @return {@code True} if enabled readThrough, writeThrough for cache.
     */
    public boolean isStoreEnabled() {
        return storeEnabled;
    }

    /**
     * @return {@code True} if enabled writeBehind for cache store.
     */
    public boolean isWriteBehind() {
        return writeBehind;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
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
        return "-cn=" + cacheName + "-b=" + backups;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TODO";
    }
}
