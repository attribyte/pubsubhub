package org.attribyte.api.pubsub.impl.server.admin.model;

import com.codahale.metrics.Metered;

import java.text.NumberFormat;

public class DisplayMetered {

   public DisplayMetered(String name, Metered metered) {
      this.name = name;
      NumberFormat decimalFormat = NumberFormat.getInstance();
      decimalFormat.setMinimumFractionDigits(2);
      decimalFormat.setMaximumFractionDigits(2);
      decimalFormat.setMinimumIntegerDigits(1);

      this.count = metered.getCount();
      this.oneMinuteRate = decimalFormat.format(metered.getOneMinuteRate());
      this.fiveMinuteRate = decimalFormat.format(metered.getFiveMinuteRate());
      this.fifteenMinuteRate = decimalFormat.format(metered.getFifteenMinuteRate());
      this.meanRate = decimalFormat.format(metered.getMeanRate());
   }

   public String getName() {
      return name;
   }

   public long getCount() {
      return count;
   }

   public String getOneMinuteRate() {
      return oneMinuteRate;
   }

   public String getFiveMinuteRate() {
      return fiveMinuteRate;
   }

   public String getFifteenMinuteRate() {
      return fifteenMinuteRate;
   }

   public String getMeanRate() {
      return meanRate;
   }

   private final String name;
   private final long count;
   private final String oneMinuteRate;
   private final String fiveMinuteRate;
   private final String fifteenMinuteRate;
   private final String meanRate;
}
