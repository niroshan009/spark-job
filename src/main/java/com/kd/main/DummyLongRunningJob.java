package com.kd.main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class DummyLongRunningJob {

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark");//.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> dummyData = new ArrayList<>();
        for (int i = 0; i < 10_000_000; i++) {
            dummyData.add(i);
        }

        JavaRDD<Integer> rdd = sc.parallelize(dummyData);

        // Simulate long processing by applying a map function with delays
        JavaRDD<Integer> processedRdd = rdd.map(item -> {
            try {
                Thread.sleep(50); // Add artificial delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Handle interruption gracefully
            }
            return item * 2; // Mock processing
        });

        // Trigger an action to execute the transformation
        processedRdd.foreachPartition(partition -> {
            // Simulate more delays in an action
            while (partition.hasNext()) {
                System.out.println(partition.next());
                Thread.sleep(10);
            }
        });

        // Hold the application for 10 minutes to allow for Kubernetes troubleshooting
        System.out.println("Job completed. Holding application for debugging...");
        Thread.sleep(10 * 60 * 1000); // 10 minutes

        sc.close();
    }
}
