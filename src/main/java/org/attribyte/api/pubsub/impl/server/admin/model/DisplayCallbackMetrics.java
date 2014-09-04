package org.attribyte.api.pubsub.impl.server.admin.model;

import org.attribyte.api.pubsub.CallbackMetrics;

import java.text.NumberFormat;

public class DisplayCallbackMetrics {

   public DisplayCallbackMetrics(String host, CallbackMetrics metrics) {
      this.host = host;

      NumberFormat decimalFormat = NumberFormat.getInstance();
      decimalFormat.setMinimumFractionDigits(2);
      decimalFormat.setMaximumFractionDigits(2);
      decimalFormat.setMinimumIntegerDigits(1);

      this.count = metrics.callbacks.getCount();
      this.rate = decimalFormat.format(metrics.callbacks.getOneMinuteRate());
      this.timing = decimalFormat.format(metrics.callbacks.getSnapshot().get95thPercentile() / 1000000.0);
      this.failedCount = metrics.failedCallbacks.getCount();
      this.failedRate = decimalFormat.format(metrics.failedCallbacks.getOneMinuteRate());
      this.abandonedCount = metrics.abandonedCallbacks.getCount();
      this.abandonedRate = decimalFormat.format(metrics.abandonedCallbacks.getOneMinuteRate());
   }

   public String getHost() {
      return host;
   }

   public long getCount() {
      return count;
   }

   public String getRate() {
      return rate;
   }

   public String getTiming() {
      return timing;
   }

   public long getFailedCount() {
      return failedCount;
   }

   public String getFailedRate() {
      return failedRate;
   }

   public long getAbandonedCount() {
      return abandonedCount;
   }

   public String getAbandonedRate() {
      return abandonedRate;
   }

   private final String host;
   private final long count;
   private final String rate;
   private final String timing;
   private final long failedCount;
   private final String failedRate;
   private final long abandonedCount;
   private final String abandonedRate;
}
