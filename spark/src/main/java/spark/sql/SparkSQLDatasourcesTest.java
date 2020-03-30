package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * spark version 2.4.0
 * hdfs version 3.0.0
 * Data Sources
 */
public class SparkSQLDatasourcesTest {

    private static final String PATH = "file:///Users/xuhuan/Desktop/softs/project/bigdatastudy/src/test/java/com/hb/bigdatastudy/spark/data/";
    private static final String PATH_OUT = "file:///Users/xuhuan/Desktop/softs/project/bigdatastudy/src/test/java/com/hb/bigdatastudy/spark/data/out/";
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

    /**
     * load/save
     */
    public void loadAndSaveDemo(){
        //parquet
        Dataset<Row> usersDF = spark.read().load(PATH + "users.parquet");
        usersDF.collect();
        usersDF.select("name", "favorite_color").write().save(PATH_OUT + "namesAndFavColors.parquet");
        //json
        Dataset<Row> peopleDF = spark.read().format("json").load(PATH + "person.json");
        peopleDF.select("name", "age").write().format("parquet").save(PATH_OUT + "namesAndAges.parquet");

        //csv
        Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(PATH + "people.csv");
    }

}

