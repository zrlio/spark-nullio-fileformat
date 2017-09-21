# Spark `nullio` File Format

Spark `nullio` file format is a no-op file format which is used for testing and benchmarking I/O and software cost
of Spark input/output routines. What does it mean? 
 
   * For output: all data written to `nullio` file format is discarded. 
   * For input: it can selectively generate particular schema data on the fly. See Generating Input Data section for more details.  

**Updates**
  * September 21, 2017: The build is moved from Spark `2.1` to `2.2`. 
  * September 21, 2017: Package rename from `com.ibm.crail.spark.sql.datasources` to `org.apache.spark.sql`
  * August 10, 2017: The initial source code release for Spark 2.1. 

## How to compile 
You can simply clone and compile it as   
```bash
 #!/bin/bash 
 set -e 
 MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
 mvn -DskipTests -T 1C  package
```
If successful, this will give you `spark-nullio-1.0.jar` in the `target` folder. There are two ways to tell Spark about 
this jar 

  (1) copy this jar into your Spark's `jar` folder. 
  (2) copy this jar to a location (lets say `/tmp/spark-nullio-1.0.jar`) and add the location to the spark 
  config (`conf/spark-defaults.conf`) as 
   ```bash
   spark.driver.extraClassPath     /tmp/*
   spark.executor.extraClassPath   /tmp/*
   ```

## How to use Spark `nullio` file format
Most common usecase of `nullio` is to decouple storage devices from the rest of the Spark I/O code path, and 
isolate performance and debugging issues in the latter. For example, by measuring the runtime of a benchmark
that writes to a stable storage (lets says HDFS) and one that just discards (using the `nullio` format), one 
can calculate how much time is spent in the actual data writing.
 
### For discarding output 
The general high-level syntax to write a Dataset in Spark (uses DataFrameWriter class[1] ) is 
```bash
scala> daatset.write.format("format").options(...).mode(savemode).save("fileName")
```
For example, to write as a parquet file (and overwrite old data [2]) this would look like 
```bash
scala> daatset.write.format("parquet").mode(SaveMode.Overwrite).save("/example.parquet")
```

Since, `nullio` is not a built in format for Spark, you have to specify the complete class path for format as 
```bash
scala> daatset.write.format("org.apache.spark.sql.NullFileFormat").save("/example.nullio")
```
and that is it. This way, whatever data was in dataset will be pushed to the `nullio` writer, that will eventually 
discard all the data. When the writer is closed, it will print some statistics. You can check them in the driver 
or executor's log. 

[1] https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter

[2] https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes

### For generating input data
Generating input data is a bit more involved. Here you can configure the `nullio` reader to generate specific 
type of schema data on the fly. There are some schema which are pre-coded, but you can add whatever you like 
as shown in the next section. 

The general high-level syntax to read input source into a Dataset (sues DataFrameReader[3]) looks something like: 
```
scala>spark.read.format("format").options(...).load("filename")
```

The input path can be configured with following parameters (you can pass them in `options`): 
  * inputrows : Number of input rows per task  (default: 1000)
  * payloadsize : size of any variable payload (default: 32 bytes)
  * intrange : the range of the integers to control the data distribution. (default: INT_MAX)
  * schema : the data schema. There are currently 3 options: (i) IntWithPayload <intKey:Int, payload:Array[Byte]>; 
  (ii) ParquetExample<intKey:Int, randLong: Long, randDouble: Double, randFloat: Float, randString: String>;
   (iii) NullSchema (NYI).

**Current limitation:** Currently, I do not have a sensible way to control the number of partitions/splits. Hence, 
the current code expects that you will pass a sensible "filename". The rest of the spark mechaniary use that 
calculate number of tasks to launch. So for example, if reading your parquet file generated 83 tasks, then 
 just changing the format type to `NullFileFormat` will still generate 83 tasks but the generated data will 
 be what you set in the options. (see above)

### How to add your own input schema and options 
In order to add your schema, please have a look into `./schema/` examples. You just have to define number of fields, 
schema, and convert your data into InternalRows using UnsafeWriters. 

## Example usage 
### Reading data from a parquet file and discarding it 
```bash
scala> val inputData = spark.read.format("parquet").load("/sampleParquet")
...
inputdata: org.apache.spark.sql.DataFrame = ...
scala> inputData.write.format("org.apache.spark.sql.NullFileFormat").save("/empty") 
...
```
Here in the executor log's (or in the dirver's log, depending upon your setting) you might see a line like this 
```bash
closing Spark(Nullio)OutputWriter. init-to-write: 489947.3 usec  runTime: 425123.88 usec  #InternalRow: 1440202 #Rows: 0 time/Internalrow: 295 nsec time/Row: 0 nsec
```
This shows that this particular writer was asked to write 1440202 `InternalRows` which it discarded. 

### Reading data from `nullio` reader and saving it  
We will generate 1001 rows (per executor) of `IntWithPayload` schema with 63 bytes of payload. 
```bash
scala> val inputData = spark.read.format("org.apache.spark.sql.NullFileFormat")
   .option("inputrows", "1001")
   .option("payloadsize", "63")
   .option("schema","intwithpayload")
   .load("/sampleParquet")
   
inputData: org.apache.spark.sql.DataFrame = [intKey: int, payload: binary]
scala> inputData.write.parquet.save("/abc.parquet")
...
scala> spark.read.parquet("file:/data/abc").count 
...
res0: Long = 2002
```
2002 is good, as I have 2 executors. 

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com