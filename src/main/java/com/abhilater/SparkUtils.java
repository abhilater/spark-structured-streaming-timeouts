package com.abhilater;

import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.SparkSession;

@Log4j
public class SparkUtils {

  public static synchronized SparkSession createSparkSession(String name) {
    return createSparkSession(name, false);
  }

  public static synchronized SparkSession createSparkSession(String name, boolean isLocal) {

    log.info("[abhilater] Actually creating spark session");
    SparkSession.Builder sparkSessionBuilder = SparkSession.builder();
    if (isLocal) {
      sparkSessionBuilder.master("local[*]");
    }

    sparkSessionBuilder.appName(name);
    SparkSession sparkSession = sparkSessionBuilder
        .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") // can use --conf setting while submitting the job as well
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    return sparkSession;
  }
}
