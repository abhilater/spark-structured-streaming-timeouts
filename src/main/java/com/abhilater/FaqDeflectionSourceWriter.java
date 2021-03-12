package com.abhilater;

import org.apache.spark.sql.SparkSession;

public class FaqDeflectionSourceWriter {

  public static final int EXPERIMENT_NUM = 3;

  public static void main(String[] args) throws Exception {
    try (SparkSession spark = SparkUtils
        .createSparkSession("TestFAQDeflectionTrigger", true)) {
      spark.conf().set("spark.sql.shuffle.partitions", 3);

      final String tableName = "faq_events" + EXPERIMENT_NUM;
      final String dataPath = "/Users/abhishek/code/Github/spark-structured-streaming-timeouts/data/";

      /**
       * Steps:
       * 1. Run with 1st 5 min window file i.e faq_events_00-05.csv
       * ts,uid,eid,type
       * 2021-03-11 10:00:00,u1,f1,f
       * 2021-03-11 10:01:00,u1,f2,f
       * 2021-03-11 10:02:00,u1,f3,f
       * 2021-03-11 10:00:00,u2,f1,f
       * 2021-03-11 10:01:00,u2,f2,f
       * 2021-03-11 10:02:00,u2,f3,f
       * 2021-03-11 10:00:00,u3,f1,f
       * 2021-03-11 10:01:00,u3,f2,f
       * 2021-03-11 10:02:00,u3,f3,f
       * 2021-03-11 10:02:00,u6,x1,x
       *
       * 2. Run with 2st 5 min window file i.e faq_events_05-10.csv
       * ts,uid,eid,type
       * 2021-03-11 10:06:00,u1,i1,i
       * 2021-03-11 10:08:00,u2,i1,i
       * 2021-03-11 10:06:00,u4,f1,f
       * 2021-03-11 10:07:00,u4,f2,f
       * 2021-03-11 10:09:00,u4,i1,i
       * 2021-03-11 10:07:00,u5,f2,f
       * 2021-03-11 10:08:00,u5,f3,f
       * 2021-03-11 10:07:00,u6,f1,f
       *
       * 3. Run with 2st 5 min window file i.e faq_events_10-15.csv
       * ts,uid,eid,type
       * 2021-03-11 10:14:00,u5,i1,i
       * 2021-03-11 10:13:00,u1,x1,y
       */
      spark.read().option("header", true)
          .csv(dataPath + "faq_events_10-15.csv")
          .write()
          .format("delta")
          .mode("append")
          .save("/tmp/delta/" + tableName);

    }
  }
}
