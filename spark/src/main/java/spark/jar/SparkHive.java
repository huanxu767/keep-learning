package spark.jar;

import org.apache.spark.sql.SparkSession;

import java.io.File;

public class SparkHive {

    public static void main(String[] args) {

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        System.out.println("warehouseLocation:" + warehouseLocation);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
//                .config("spark.sql.warehouse.dir", warehouseLocation)
                .getOrCreate();

//        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
//        spark.sql("LOAD DATA LOCAL INPATH '"+PATH+"/kv1.txt' INTO TABLE src");
        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM `test`.`xh_external_table` ").show();
        // +---+-------+
        // |key|  value|
        // +---+-------+
        // |238|val_238|
        // | 86| val_86|
        // |311|val_311|
        // ...

        // Aggregation queries are also supported.
        spark.sql("SELECT COUNT(*) FROM `test`.`xh_external_table`").show();
        // +--------+
        // |count(1)|
        // +--------+
        // |    500 |
        // +--------+

        // The results of SQL queries are themselves DataFrames and support all normal functions.
//        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");
//
//        // The items in DataFrames are of type Row, which lets you to access each column by ordinal.
//        Dataset<String> stringsDS = sqlDF.map(
//                (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
//                Encoders.STRING());
//        stringsDS.show();
        // +--------------------+
        // |               value|
        // +--------------------+
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // |Key: 0, Value: val_0|
        // ...

        // You can also use DataFrames to create temporary views within a SparkSession.
//        List<Record> records = new ArrayList<>();
//        for (int key = 1; key < 100; key++) {
//            Record record = new Record();
//            record.setKey(key);
//            record.setValue("val_" + key);
//            records.add(record);
//        }
//        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
//        recordsDF.createOrReplaceTempView("records");

        // Queries can then join DataFrames data with data stored in Hive.
//        spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
        // +---+------+---+------+
        // |key| value|key| value|
        // +---+------+---+------+
        // |  2| val_2|  2| val_2|
        // |  2| val_2|  2| val_2|
        // |  4| val_4|  4| val_4|
        // ...
    }

}

