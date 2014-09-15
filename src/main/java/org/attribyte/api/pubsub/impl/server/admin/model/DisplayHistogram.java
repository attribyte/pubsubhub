package org.attribyte.api.pubsub.impl.server.admin.model;

import com.codahale.metrics.Histogram;

import java.text.NumberFormat;

public class DisplayHistogram {

   public DisplayHistogram(String name, Histogram histogram) {

      NumberFormat decimalFormat = NumberFormat.getInstance();
      decimalFormat.setMinimumFractionDigits(2);
      decimalFormat.setMaximumFractionDigits(2);
      decimalFormat.setMinimumIntegerDigits(1);

      this.mean = decimalFormat.format(histogram.getSnapshot().getMean());
      this.p50 = decimalFormat.format(histogram.getSnapshot().getMedian());
      this.p75 = decimalFormat.format(histogram.getSnapshot().get75thPercentile());
      this.p95 = decimalFormat.format(histogram.getSnapshot().get95thPercentile());
      this.p98 = decimalFormat.format(histogram.getSnapshot().get98thPercentile());
      this.p99 = decimalFormat.format(histogram.getSnapshot().get99thPercentile());
      this.p999 = decimalFormat.format(histogram.getSnapshot().get999thPercentile());
      this.std = decimalFormat.format(histogram.getSnapshot().getStdDev());
   }

   public String getMean() {
      return mean;
   }

   public String getP50() {
      return p50;
   }

   public String getP75() {
      return p75;
   }

   public String getP95() {
      return p95;
   }

   public String getP98() {
      return p98;
   }

   public String getP99() {
      return p99;
   }

   public String getP999() {
      return p999;
   }

   public String getStd() {
      return std;
   }

   private final String mean;
   private final String p50;
   private final String p75;
   private final String p95;
   private final String p98;
   private final String p99;
   private final String p999;
   private final String std;
}
