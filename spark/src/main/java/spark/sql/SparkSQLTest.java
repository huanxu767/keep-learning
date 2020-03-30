package spark.sql;



import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import spark.model.Person;
import spark.udf.MyAverage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

/**]]]
 * spark version 2.4.0
 * hdfs version 3.0.0
 */
public class SparkSQLTest {

    private static final String PATH = "file:///Users/xuhuan/Desktop/softs/project/bigdatastudy/spark/src/main/resources/data";

    private SparkSession spark;
    public void before() {
        System.setProperty("hadoop.home.dir", "D:\\Develop\\hadoop-2.6.0");
        String appName = "test_sql";
        spark = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

    }

    public void datasetDemo() {
        //json 文件整体不需要是纯json格式，按行放置JSON文件
        Dataset<Row> df = spark.read().json(PATH + "person.json");
        df.printSchema();
        df.select("name").show();
//        df.select(df.col("name"), df.col("age").plus(1)).show();
        df.select(col("name"), col("age").plus(1)).show();
        df.filter(col("age").gt(21)).show();
        df.groupBy("age").count().show();

        df.show();
        df.createOrReplaceTempView("employee");
        Dataset<Row> resultDataSet = spark.sql("select * from employee where id = 1202");
        resultDataSet.show(false);
    }

    /**
     * json
     */
    public void encoderDemo() {
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = PATH + "person.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

    }

    /**
     * Spark SQL supports automatically converting an RDD of JavaBeans into a DataFrame
     */
    public void convertRddToDataFrameDemo() {
        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(PATH + "person.txt").javaRDD()
                .map(line ->{
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                }
        );
        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD,Person.class);

        peopleDF.show();
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");
        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 27 AND 35");
        teenagersDF.show();
        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();

        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        // or by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
    }
    /**
     * Programmatically Specifying the Schema
     * When JavaBean classes cannot be defined ahead of time (for example, the structure of records is encoded in a string,
     * or a text dataset will be parsed and fields will be projected differently for different users),
     * a Dataset<Row> can be created programmatically with three steps.
     *
     * 1.Create an RDD of Rows from the original RDD;
     * 2.Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
     * 3.Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
     */
    public void specifyingTheSchema(){
        JavaRDD<String> peopleRDD= spark.read().textFile(PATH + "person.txt").javaRDD();
        System.out.println(peopleRDD.collect());
        String schemaString = "name age";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")){
            StructField structField = DataTypes.createStructField(fieldName,DataTypes.StringType,true);
            fields.add(structField);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String,Row>) record ->{
           String[] attributes = record.split(",");
           return RowFactory.create(attributes[0],attributes[1].trim());
        });

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD,schema);
        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT name FROM people where age > 30");
        results.show();
        // The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();

    }

    /**
     * Aggregations
     * 自定义UDF函数
     */
    public void averageDemo(){
        spark.udf().register("myAverage", new MyAverage());
        Dataset<Row> df = spark.read().json(PATH + "person.json");
        df.createOrReplaceTempView("people");
        df.show();
        Dataset<Row> result = spark.sql("SELECT myAverage(age) as average_age FROM people");
        result.show();
    }

}

