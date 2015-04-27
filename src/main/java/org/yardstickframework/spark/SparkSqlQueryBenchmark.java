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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.storage.*;
import org.yardstickframework.*;
import org.yardstickframework.spark.model.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Ignite benchmark that performs query operations.
 */
public class SparkSqlQueryBenchmark extends SparkAbstractBenchmark {
    /** */
    public static final String TABLE_NAME = "person";

    /** Sql context. */
    private SQLContext sqlContext;

    /** */
    private static final DecimalFormat format = new DecimalFormat("##.##");

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        List<PersonLight> persons = new ArrayList<>(args.range());

        for (int i = 0; i < args.range(); i++) {
            persons.add(new PersonLight(i, i * 1000));

            if (i % 100000 == 0)
               println(cfg, "Populated persons: " + i);
        }

        JavaRDD<PersonLight> rdds = sc.textFile("file:///./config/person.txt").map(new Function<String, PersonLight>() {
            @Override
            public PersonLight call(String input) throws Exception {
                String[] split = input.split(" ");

                return new PersonLight(Integer.valueOf(split[0]), Double.valueOf(split[1]));
            }
        });

        sqlContext = new SQLContext(sc);

        DataFrame dataFrame = sqlContext.createDataFrame(rdds, PersonLight.class);

        dataFrame.repartition(3);

        dataFrame.registerTempTable(TABLE_NAME);

        if (args.backups())
            dataFrame.persist(StorageLevel.MEMORY_ONLY_2());
        else
            dataFrame.persist(StorageLevel.MEMORY_ONLY());

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        double salary = ThreadLocalRandom.current().nextDouble() * args.range() * 1000;

        double maxSalary = salary + 1000;

        /*Collection<Row> entries = */executeQuery(salary, maxSalary);

//        for (Row entry : entries) {
//            Double entrySalary = entry.getDouble(1);
//
//            if (entrySalary < salary || entrySalary > maxSalary)
//                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
//                        ", person=" + entrySalary + ']');
//        }

        return true;
    }

    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query result.
     * @throws Exception If failed.
     */
    private void executeQuery(double minSalary, double maxSalary) throws Exception {
        sqlContext.sql("SELECT id, salary FROM " + TABLE_NAME + " WHERE salary >= "
            + format.format(minSalary) + " AND salary <= " + format.format(maxSalary))
                .save("./test/res.txt", SaveMode.Append);
    }
}
