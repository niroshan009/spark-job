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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PersonJob {

//    yLyiS0vaXeeZDnW9QvP9
//    Swu0zOG7yLRWNbLaj4Y3aJKbsuuN2Z4VRaguhwdO

    private static final String AWS_KEY = "GcpHcawcrlip6n2OTzRr";
    private static final String AWS_SECRET_KEY = "DSywG5vYA3HpIl3X7DFVnbqaBL3XAQfa6RXkYkGg";

    public static void main(String[] args) throws NoSuchTableException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .appName("JavaIcebergJob")
                .master("local[*]") // remove this if you're running on a cluster
                .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.demo.type", "hadoop")
                .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse")
                .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")

                .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();


        spark.sql("CREATE TABLE demo.person.information_2 (id INT, name STRING) USING ICEBERG");


        spark.sql("INSERT INTO demo.person.information_ VALUES (1, 'Alice', 'wonder', 'alice@gmail.com', 'female', '127.0.0.1')");

    }
}
