# Yardstick Apache Spark Benchmarks
Yardstick Apache Spark is a set of <a href="https://spark.apache.org" target="_blank">Apache Spark</a> benchmarks written on top of Yardstick framework.

## Yardstick Framework
Visit <a href="https://github.com/yardstick-benchmarks/yardstick" target="_blank">Yardstick Repository</a> for detailed information on how to run Yardstick benchmarks and how to generate graphs.

The documentation below describes configuration parameters in addition to standard Yardstick parameters.

## Installation
1. Create a local clone of Yardstick Apache Spark repository
2. Import Yardstick Apache Spark POM file into your project
3. Run `mvn package` command

## Provided Benchmarks
The following benchmarks are provided:

1. `SparkSqlQueryBenchmark` - benchmark sql query operations.
2. `SparkQueryDSLBenchmark` - benchmarks query dsl operations.

## Writing Apache Spark Benchmarks
All benchmarks extend `SparkAbstractBenchmark` class. A new benchmark should also extend this abstract class and implement `test` method. This is the method that is actually benchmarked.

## Running Apache Spark Benchmarks
Before running Apache Spark benchmarks, run `mvn package` command. This command will compile the project and also will unpack scripts from `yardstick-resources.zip` file to `bin` directory.

### Properties And Command Line Arguments
> Note that this section only describes configuration parameters specific to Apache Spark benchmarks, and not for Yardstick framework. To run Apache Spark benchmarks and generate graphs, you will need to run them using Yardstick framework scripts in `bin` folder.

> Refer to [Yardstick Documentation](https://github.com/gridgain/yardstick) for common Yardstick properties and command line arguments for running Yardstick scripts.

The following benchmark properties can be defined in the benchmark configuration:

* `-b` or `--backups` - Set storage level `MEMORY_ONLY_2` (replicate each partition on two cluster nodes). By default `MEMORY_ONLY`.

## License
Yardstick Apache Spark is available under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Open Source license.