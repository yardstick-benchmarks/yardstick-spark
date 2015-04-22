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
import org.apache.spark.sql.*;
import org.apache.spark.storage.*;
import org.yardstickframework.*;
import org.yardstickframework.spark.model.*;

import java.text.*;
import java.util.*;
import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Ignite benchmark that performs query operations.
 */
public class SparkSqlJoinBenchmark extends SparkAbstractBenchmark {
    /** */
    public static final String PERSON_TABLE_NAME = "person";

    /** */
    public static final String ORGANIZATION_TABLE_NAME = "organization";

    /** Sql context. */
    private SQLContext sqlContext;

    /** */
    private static final DecimalFormat format = new DecimalFormat("##.##");

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        // Populate organizations.
        int orgRange = args.range() / 10;

        List<Organization> orgs = new ArrayList<>(orgRange);

        for (int i = 0; i < orgRange; i++)
            orgs.add(new Organization(i, "org" + i));

        // Populate persons.
        List<Person> persons = new ArrayList<>(args.range());

        for (int i = 0; i < args.range(); i++)
            persons.add(new Person(i, nextRandom(orgRange),"firstName" + i, "lastName" + i, i * 1000));

        JavaRDD<Person> personRdd = sc.parallelize(persons);

        JavaRDD<Organization> orgsRdd = sc.parallelize(orgs);

        if (args.backups()) {
            personRdd.persist(StorageLevel.MEMORY_ONLY_2());

            orgsRdd.persist(StorageLevel.MEMORY_ONLY_2());
        }
        else {
            personRdd.persist(StorageLevel.MEMORY_ONLY());

            orgsRdd.persist(StorageLevel.MEMORY_ONLY());
        }

        sqlContext = new SQLContext(sc);

        DataFrame personDataFrame = sqlContext.createDataFrame(personRdd, Person.class);
        personDataFrame.registerTempTable(PERSON_TABLE_NAME);

        sqlContext.cacheTable(PERSON_TABLE_NAME);

        DataFrame orgDataFrame = sqlContext.createDataFrame(orgsRdd, Organization.class);
        orgDataFrame.registerTempTable(ORGANIZATION_TABLE_NAME);

        sqlContext.cacheTable(ORGANIZATION_TABLE_NAME);

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        double salary = ThreadLocalRandom.current().nextDouble() * args.range() * 1000;

        double maxSalary = salary + 1000;

        Collection<Row> entries = executeQuery(salary, maxSalary);

        for (Row row : entries) {
            double sal = row.getDouble(4);

            if (sal < salary || sal > maxSalary) {
                Person p = new Person();

                p.setId(row.getInt(0));
                p.setOrgId(row.getInt(1));
                p.setFirstName(row.getString(2));
                p.setLastName(row.getString(3));
                p.setSalary(sal);

                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                    ", person=" + p + ']');
            }
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
        return sqlContext.sql("SELECT p.id, p.orgId, p.firstName, p.lastName, p.salary, o.name " +
            "FROM " + PERSON_TABLE_NAME + " p " +
            "LEFT JOIN " + ORGANIZATION_TABLE_NAME + " o " +
            "ON p.id = o.id " +
            "WHERE salary >= " + format.format(minSalary) + " AND salary <= " + format.format(maxSalary))
                .collectAsList();
    }
}
