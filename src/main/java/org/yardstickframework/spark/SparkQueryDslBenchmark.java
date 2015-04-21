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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.spark.model.Person;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations.
 */
public class SparkQueryDslBenchmark extends SparkAbstractBenchmark {
    /** */
    public static final String TABLE_NAME = "person";

    /** Data frame. */
    private DataFrame df;

    private static final DecimalFormat format = new DecimalFormat("##.##");

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        List<Person> persons = new ArrayList<>(args.range());

        for (int i = 0; i < args.range(); i++) {
            persons.add(new Person(i, "firstName" + i, "lastName" + i, i * 1000));

            if (i % 100000 == 0)
               println(cfg, "Populated persons: " + i);
        }

        JavaRDD<Person> rdds = sc.parallelize(persons);

        if (args.backups())
            rdds.persist(StorageLevel.MEMORY_ONLY_2());
        else
            rdds.persist(StorageLevel.MEMORY_ONLY());

        SQLContext sqlContext = new SQLContext(sc);

        df = sqlContext.createDataFrame(rdds, Person.class);
        df.registerTempTable(TABLE_NAME);

        sqlContext.cacheTable(TABLE_NAME);

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        double salary = ThreadLocalRandom.current().nextDouble() * args.range() * 1000;

        double maxSalary = salary + 1000;

        Collection<Row> entries = executeQuery(salary, maxSalary);

        for (Row entry : entries) {
            Double entrySalary = entry.getDouble(1);

            if (entrySalary < salary || entrySalary > maxSalary)
                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                        ", person=" + entrySalary + ']');
        }

        return true;
    }

    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query result.
     * @throws Exception If failed.
     */
    private Collection<Row> executeQuery(double minSalary, double maxSalary) throws Exception {
        return df.filter(df.col("salary").gt(minSalary).and(df.col("salary").lt(maxSalary)))
            .select("firstName", "salary").collectAsList();
    }
}
