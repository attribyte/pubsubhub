package org.attribyte.api.pubsub;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import org.attribyte.essem.metrics.Timer;

import java.util.Comparator;
import java.util.Map;

/**
 * Holds metrics associated with a callback.
 */
public class CallbackMetrics implements MetricSet {

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
      THROUGHPUT_ASC,

      /**
       * Sort by most failures.
       */
      FAILURE_RATE_DESC,

      /**
       * Sort by least failures.
       */
      FAILURE_RATE_ASC,

      /**
       * Sort by most failures.
       */
      ABANDONED_RATE_ASC,

      /**
       * Sort by least failures.
       */
      ABANDONED_RATE_DESC

   }


   /**
    * Sort by lowest throughput.
    */
   public static final Comparator<CallbackMetrics> throughputAscendingComparator = new Comparator<CallbackMetrics>() {
      @Override
      public int compare(final CallbackMetrics o1, final CallbackMetrics o2) {
         double r1 = o1.callbacks.getFiveMinuteRate();
         double r2 = o2.callbacks.getFiveMinuteRate();
         return Double.compare(r1, r2);
      }
   };

   /**
    * Sort by lowest failure rate.
    */
   public static final Comparator<CallbackMetrics> failureRateAscendingComparator = new Comparator<CallbackMetrics>() {
      @Override
      public int compare(final CallbackMetrics o1, final CallbackMetrics o2) {
         double r1 = o1.failedCallbacks.getFiveMinuteRate();
         double r2 = o2.failedCallbacks.getFiveMinuteRate();
         return Double.compare(r1, r2);
      }
   };

   /**
    * Sort by lowest abandoned rate.
    */
   public static final Comparator<CallbackMetrics> abandonedRateAscendingComparator = new Comparator<CallbackMetrics>() {
      @Override
      public int compare(final CallbackMetrics o1, final CallbackMetrics o2) {
         double r1 = o1.abandonedCallbacks.getFiveMinuteRate();
         double r2 = o2.abandonedCallbacks.getFiveMinuteRate();
         return Double.compare(r1, r2);
      }
   };

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.<String, Metric>builder()
              .put("time-to-callback", timeToCallback)
              .put("callbacks", callbacks)
              .put("failed-callbacks", failedCallbacks)
              .put("abandoned-callbacks", abandonedCallbacks).build();
   }

   /**
    * Tracks the time spent from notification to callback completion.
    */
   public final Timer timeToCallback = new Timer();

   /**
    * Times all callbacks.
    */
   public final Timer callbacks = new Timer();

   /**
    * Tracks the rate of failed callbacks.
    */
   public final Meter failedCallbacks = new Meter();

   /**
    * Tracks the rate of abandoned callbacks.
    */
   public final Meter abandonedCallbacks = new Meter();

}
