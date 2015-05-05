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

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.yardstickframework.*;
import org.yardstickframework.spark.model.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Spark benchmark that performs streaming operations.
 */
public class SparkStreamingBenchmark extends SparkAbstractBenchmark {
    /** */
    private JavaStreamingContext jssc;

    /** */
    private Queue<JavaRDD<SampleValue>> data;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        List<SampleValue> batch = new ArrayList<>(args.batchSize());

        for (int i = 0; i <= args.batchSize(); i++)
            batch.add(new SampleValue(i));

        data = new ArrayDeque<>(8);

        JavaRDD<SampleValue> rdd = sc.parallelize(batch);

        // Init collection.
        rdd.count();

        data.add(rdd);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        jssc = new JavaStreamingContext(sc, Duration.apply(10));

        JavaDStream<SampleValue> stream = jssc.queueStream(data, true);

        final int filterArg = nextRandom(args.batchSize() - 1);

        final int size = args.batchSize() - filterArg;

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger counter = new AtomicInteger(0);

        stream.map(new ToId()).filter(new GreaterThan(filterArg)).foreachRDD(
            new Function<JavaRDD<Integer>, Void>() {
                @Override public Void call(JavaRDD<Integer> v1) throws Exception {
                    // to make sure that all data processed.
                    if (counter.addAndGet((int) v1.count()) >= size)
                        latch.countDown();

                    return null;
                }
            });

        jssc.start();

        latch.await();

        // Stopping streaming.
        jssc.stop(false, false);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        super.tearDown();

        jssc.stop(false);
    }

    /**
     * Converter.
     */
    public static class ToId implements Function<SampleValue, Integer>, Externalizable {
        /** */
        public ToId() {
            // No-op.
        }

        @Override public Integer call(SampleValue v1) throws Exception {
            return v1.getId();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    /**
     * Filter "greater than".
     */
    public static class GreaterThan implements Function<Integer, Boolean>, Externalizable {
        /** */
        private int digit;

        /** */
        public GreaterThan() {
            // No-op.
        }

        /** */
        public GreaterThan(int digit) {
            this.digit = digit;
        }

        @Override public Boolean call(Integer v1) throws Exception {
            return digit < v1;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(digit);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            digit = in.readInt();
        }
    }
}
