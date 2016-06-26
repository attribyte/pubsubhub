package org.attribyte.api.pubsub;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import org.attribyte.essem.metrics.HDRReservoir;
import org.attribyte.essem.metrics.Timer;

import java.util.Comparator;
import java.util.Map;

/**
 * Holds metrics associated with a notification.
 */
public class NotificationMetrics implements MetricSet {

   /**
    * Specifies sort for metrics.
    */
   public enum Sort {

      /**
       * Sort by highest throughput.
       */
      THROUGHPUT_DESC,

      /**
       * Sort by lowest throughput.
       */
      THROUGHPUT_ASC
   }

   /**
    * Creates the metrics.
    * @param topicId The associated topic id.
    */
   public NotificationMetrics(final long topicId) {
      this.topicId = topicId;
   }

   /**
    * Sort by lowest throughput.
    */
   public static final Comparator<NotificationMetrics> throughputAscendingComparator = new Comparator<NotificationMetrics>() {
      @Override
      public int compare(final NotificationMetrics o1, final NotificationMetrics o2) {
         double r1 = o1.notifications.getFiveMinuteRate();
         double r2 = o2.notifications.getFiveMinuteRate();
         return Double.compare(r1, r2);
      }
   };

   @Override
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("notifications", notifications);
      builder.put("notification-size", notificationSize);
      return builder.build();
   }

   /**
    * Times notification accept/rate.
    */
   public final Timer notifications = new Timer();

   /**
    * A histogram of notification size.
    */
   public final Histogram notificationSize = new Histogram(new HDRReservoir(2, HDRReservoir.REPORT_SNAPSHOT_HISTOGRAM));

   /**
    * The topic id.
    */
   public final long topicId;
}