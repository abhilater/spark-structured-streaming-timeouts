package com.abhilater;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

import com.abhilater.beans.FAQDeflectionOutput;
import  com.abhilater.beans.FAQEvent;
import  com.abhilater.beans.UserFAQState;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.log4j.Log4j;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

@Log4j
public class FaqDeflectionPOC {

  public static void main(String[] args) throws Exception {
    try (SparkSession spark = SparkUtils
        .createSparkSession("TestFAQDeflection", true)) {

      spark.conf().set("spark.sql.shuffle.partitions", 3);

      // query to generate intermediate flatMapGroup records with TimeOut and State logic for Deflection metrics
      spark.readStream().format("delta")
          .load("/tmp/delta/faq_events" + FaqDeflectionSourceWriter.EXPERIMENT_NUM)
          .selectExpr("to_timestamp(ts, 'yyyy-MM-dd HH:mm:ss') as eventTime", "uid", "eid", "type")
          .where("type in ('i','f')")
          .withWatermark("eventTime", "5 minute")
          .as(Encoders.bean(FAQEvent.class))
          .groupByKey((MapFunction<FAQEvent, String>) FAQEvent::getUid, Encoders.STRING())
          .flatMapGroupsWithState(flatMapStateUpdateFunc,
              OutputMode.Append(),
              Encoders.bean(UserFAQState.class),
              Encoders.bean(FAQDeflectionOutput.class),
              GroupStateTimeout.EventTimeTimeout())
          .writeStream()
          .option("checkpointLocation",
              "/tmp/cp/faq_deflection" + FaqDeflectionSourceWriter.EXPERIMENT_NUM)
          .format("delta")
          .outputMode("append")
          .trigger(Trigger.Once())
          .start("/tmp/delta/faq_flatmapgroup_events" + FaqDeflectionSourceWriter.EXPERIMENT_NUM)
          .awaitTermination();

      // query to store intermediate flatMapGroup records in a Delta table
      spark.readStream().format("delta")
          .load("/tmp/delta/faq_flatmapgroup_events" + FaqDeflectionSourceWriter.EXPERIMENT_NUM)
          .writeStream()
          .option("checkpointLocation",
              "/tmp/cp/faq_deflection_intermediate" + FaqDeflectionSourceWriter.EXPERIMENT_NUM)
          .format("console")
          .outputMode("append")
          .option("truncate", false)
          .trigger(Trigger.Once())
          .start()
          .awaitTermination();

      // query to do 5 min time window aggregation on intermediate records from Delta table
      spark.readStream().format("delta")
          .load("/tmp/delta/faq_flatmapgroup_events" + FaqDeflectionSourceWriter.EXPERIMENT_NUM)
          .groupBy(window(col("eventTime"), "5 minutes"), col("eid"))
          .agg(sum("SDefCount").as("successful_deflections"),
              sum("FDefCount").as("failed_deflections"))
          .writeStream()
          .option("checkpointLocation", "/tmp/cp/faq_deflection_out" + FaqDeflectionSourceWriter.EXPERIMENT_NUM)
          .format("console")
          .outputMode("update")
          .option("truncate", false)
          .trigger(Trigger.Once())
          .start()
          .awaitTermination();

    }
  }

  static final FlatMapGroupsWithStateFunction<String, FAQEvent, UserFAQState, FAQDeflectionOutput> flatMapStateUpdateFunc =
      (FlatMapGroupsWithStateFunction<String, FAQEvent, UserFAQState, FAQDeflectionOutput>) (uid, events, oldState) -> {
        // sort the input events by event time
        List<FAQEvent> eventsSorted = StreamSupport
            .stream(Spliterators.spliteratorUnknownSize(events, Spliterator.ORDERED), false)
            .sorted(Comparator.comparing(FAQEvent::getEventTime))
            .collect(Collectors.toList());
        List<FAQDeflectionOutput> output = new ArrayList<>();

        if (uid.equals("u3") || uid.equals("u2")) {
          log.info("uid: " + uid);
          log.info("events: " + events);
          log.info("oldState.hasTimedOut(): " + oldState.hasTimedOut());
          log.info("oldState.exists(): " + oldState.exists());
          log.info("oldState.get(): " + (oldState.exists() ? oldState.get() : null));
        }
        // hope that timed out group that has no data in stream will be intercepted
        if (oldState.hasTimedOut() && "f".equals(oldState.get().getPrevActionType())) {
          UserFAQState s = oldState.get();
          log.info("oldState.hasTimedOut() " + s);
          // emit event for timed out group
          output.add(
              new FAQDeflectionOutput(s.getTs(), s.getPrevEid(), 1, 0, s.getTs(), s.getPrevEid(),
                  uid));
          oldState.remove();
        }

        UserFAQState state = oldState.exists() ? oldState.get() : null;

        for (FAQEvent event : eventsSorted) {
          if (null == state) {
            // do nothing
          } else if ("i".equals(event.getType()) &&
              "f".equals(state.getPrevActionType()) &&
              toMinutes(event.getEventTime().getTime() - state.getTs().getTime()) > 5L) {

            output.add(new FAQDeflectionOutput(state.getTs(), state.getPrevEid(), 1, 0,
                event.getEventTime(), event.getEid(), uid));
          } else if ("i".equals(event.getType()) &&
              "f".equals(state.getPrevActionType()) &&
              toMinutes(event.getEventTime().getTime() - state.getTs().getTime()) <= 5L) {

            output.add(new FAQDeflectionOutput(state.getTs(), state.getPrevEid(), 0, 1,
                event.getEventTime(), event.getEid(), uid));
          }
          state = new UserFAQState(uid, event.getEid(), event.getType(), event.getEventTime());
        }

        long wm = oldState.getCurrentWatermarkMs();
        if (uid.equals("u3") || uid.equals("u2")) {
          log.info("Old watermark: " + wm);
        }
        if (null != state) {
          oldState.update(state);
          oldState.setTimeoutTimestamp(wm, "10 seconds"); // as I want it to timeout 10 seconds after Watermark which is anyway 5 mins past
        }
        return output.iterator();
      };

  static long toMinutes(long millis) {
    return TimeUnit.MILLISECONDS.toMinutes(millis);
  }

  /**
   * 1st Batch - No events are candidates for successful or failed deflection as there is no 'i' event type and only 'f' event types
   * -------------------------------------------
   * Batch: 0
   * -------------------------------------------
   * +---------+---------+----------+----------------+---+---------+---+
   * |FDefCount|SDefCount|currentEid|currentEventTime|eid|eventTime|uid|
   * +---------+---------+----------+----------------+---+---------+---+
   * +---------+---------+----------+----------------+---+---------+---+
   *
   * [2021-03-11 18:52:06,481] WARN  org.apache.spark.sql.execution.streaming.MicroBatchExecution: The read limit MaxFiles: 1000 for DeltaSource[file:/tmp/delta/faq_flatmapgroup_events1] is ignored when Trigger.Once() is used.
   * -------------------------------------------
   * Batch: 0
   * -------------------------------------------
   * +------+---+----------------------+------------------+
   * |window|eid|successful_deflections|failed_deflections|
   * +------+---+----------------------+------------------+
   * +------+---+----------------------+------------------+
   *
   * 2nd Batch - 2 successful and 2 failed deflections
   * for u1,u2,u4 these are due to explicit 'i' events and time difference between last 'f' events from State store
   * but Hurray!!! for u3 there is no event in this group and this event is emitted due to GroupStateTimeout logic
   * -------------------------------------------
   * Batch: 1
   * -------------------------------------------
   * +---------+---------+----------+-------------------+---+-------------------+---+
   * |FDefCount|SDefCount|currentEid|currentEventTime   |eid|eventTime          |uid|
   * +---------+---------+----------+-------------------+---+-------------------+---+
   * |1        |0        |i1        |2021-03-11 10:09:00|f2 |2021-03-11 10:07:00|u4 |
   * |1        |0        |i1        |2021-03-11 10:06:00|f3 |2021-03-11 10:02:00|u1 |
   * |0        |1        |i1        |2021-03-11 10:08:00|f3 |2021-03-11 10:02:00|u2 |
   * |0        |1        |f3        |2021-03-11 10:02:00|f3 |2021-03-11 10:02:00|u3 |
   * +---------+---------+----------+-------------------+---+-------------------+---+
   *
   * -------------------------------------------
   * Batch: 1
   * -------------------------------------------
   * +------------------------------------------+---+----------------------+------------------+
   * |window                                    |eid|successful_deflections|failed_deflections|
   * +------------------------------------------+---+----------------------+------------------+
   * |[2021-03-11 10:00:00, 2021-03-11 10:05:00]|f3 |2                     |1                 |
   * |[2021-03-11 10:05:00, 2021-03-11 10:10:00]|f2 |0                     |1                 |
   * +------------------------------------------+---+----------------------+------------------+
   *
   * 3nd Batch - 1 successful and 1 failed deflections
   * for u5 these are due to explicit 'i' events and time difference between last 'f' events from State store
   * but Hurray!!! for u6 there is no event in this group and this event is emitted due to GroupStateTimeout logic
   * -------------------------------------------
   * Batch: 2
   * -------------------------------------------
   * +---------+---------+----------+-------------------+---+-------------------+---+
   * |FDefCount|SDefCount|currentEid|currentEventTime   |eid|eventTime          |uid|
   * +---------+---------+----------+-------------------+---+-------------------+---+
   * |0        |1        |i1        |2021-03-11 10:14:00|f3 |2021-03-11 10:08:00|u5 |
   * |0        |1        |f1        |2021-03-11 10:07:00|f1 |2021-03-11 10:07:00|u6 |
   * +---------+---------+----------+-------------------+---+-------------------+---+
   *
   * -------------------------------------------
   * Batch: 2
   * -------------------------------------------
   * +------------------------------------------+---+----------------------+------------------+
   * |window                                    |eid|successful_deflections|failed_deflections|
   * +------------------------------------------+---+----------------------+------------------+
   * |[2021-03-11 10:05:00, 2021-03-11 10:10:00]|f3 |1                     |0                 |
   * |[2021-03-11 10:05:00, 2021-03-11 10:10:00]|f1 |1                     |0                 |
   * +------------------------------------------+---+----------------------+------------------+
   */

}
