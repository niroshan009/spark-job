package com.kd.main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

public class PersonJob {

//    yLyiS0vaXeeZDnW9QvP9
//    Swu0zOG7yLRWNbLaj4Y3aJKbsuuN2Z4VRaguhwdO

    private static final String AWS_KEY = "yLyiS0vaXeeZDnW9QvP9";
    private static final String AWS_SECRET_KEY = "Swu0zOG7yLRWNbLaj4Y3aJKbsuuN2Z4VRaguhwdO";

    public static void main(String[] args) throws NoSuchTableException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc);
        SparkSession sparkSession = SparkSession.builder()
                .appName("testsql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///~/tmp")
                .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.demo","org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.demo.type", "rest")
                .config("spark.sql.catalog.demo.uri","http://127.0.0.1:8181")
                .config("spark.sql.catalog.demo.io-impl","org.apache.iceberg.hadoop.HadoopFileIO")
                .config("spark.sql.catalog.demo.warehouse","s3://warehouse/wh/")
                .config("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000")
                .config("spark.sql.defaultCatalog", "demo")
                .config("spark.eventLog.enabled", "true")
                .getOrCreate();
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", AWS_KEY);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY);
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");



//        input-files/person-information.csv
        String input_path = "s3a://input-files/person-information.csv";
        Dataset<Row> csvData = sparkSession.read()
                .option("header", "true") // if your CSV has a header
                .option("inferSchema", "true")
                .csv(input_path);

        csvData.show(false);

        csvData.writeTo("tabular.your_namespace.your_table_name")
                .append();
    }
}
