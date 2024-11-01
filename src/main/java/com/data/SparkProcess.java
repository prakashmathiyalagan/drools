package com.data;

import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

public class SparkProcess {
    private static Logger logger = Logger.getLogger(SparkProcess.class.getName());

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        logger.info("Spark session: " + sparkSession);
    }
}
