package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import org.attribyte.api.InitializationException;
import org.attribyte.api.pubsub.DisableSubscriptionStrategy;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.util.InitUtil;

import java.util.Properties;

/**
 * Disable subscriptions based on the fraction of failed and/or abandoned callbacks.
 * <p>Properties</p>
 * <ul>
 * <li>callbackThreshold - The minimum number of callbacks before disabling is considered.</li>
 * <li>failedThreshold - The minimum number of failed callbacks before disabling is considered.</li>
 * <li>window - The window used for calculating ratios for disable (oneMinute, fiveMinute, fifteenMinute).</li>
 * <li>abandonedThreshold - The minimum number of abandoned callbacks before disabling is considered.</li>
 * <li>failedFraction - The fraction of failed callbacks in the window above which the subscription will be disabled.</li>
 * <li>abandonedFraction - the fraction of abandoned callbacks in the window, above which the subscription will be disabled.</li>
 * </ul>
 */
public class SubscriptionDisabler implements DisableSubscriptionStrategy {

   /**
    * The window used for rate measurements.
    */
   private static enum RateWindow {
      ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE
   }

   @Override
   public boolean disableSubscription(final Subscription subscription,
                                      final Metered callbackMeter,
                                      final Meter failedCallbackMeter, final Meter abandonedCallbackMeter) {

      long callbackCount = callbackMeter.getCount();
      if(callbackCount <= callbackThreshold) {
         return false;
      }

      long failedCount = failedCallbackMeter.getCount();
      if(failedCount <= failedThreshold) {
         return false;
      }

      long abandonedCount = abandonedCallbackMeter.getCount();
      if(abandonedCount <= abandonedThreshold) {
         return false;
      }

      double callbackRate = getRate(callbackMeter);
      double failedRate = getRate(failedCallbackMeter);
      double abandonedRate = getRate(abandonedCallbackMeter);

      if(callbackRate > 0.0f) {
         double failedRatio = failedRate / callbackRate;
         if(failedRatio > failedFractionThreshold) {
            return true;
         }
         double abandonedRatio = abandonedRate / callbackRate;
         if(abandonedRatio > abandonedFractionThreshold) {
            return true;
         }
      }

      return false;
   }

   @Override
   public void init(final Properties props) throws InitializationException {
      InitUtil init = new InitUtil("subscriptionDisabler.", props, false); //Don't lowercase names
      callbackThreshold = init.getIntProperty("callbackThreshold", 0);
      failedThreshold = init.getIntProperty("failedThreshold", 0);
      abandonedThreshold = init.getIntProperty("abandonedThreshold", 0);
      String rateWindowStr = init.getProperty("window", "fiveMinute");
      if(rateWindowStr.equalsIgnoreCase("oneMinute")) {
         rateWindow = RateWindow.ONE_MINUTE;
      } else if(rateWindowStr.equalsIgnoreCase("fifteenMinute")) {
         rateWindow = RateWindow.FIFTEEN_MINUTE;
      } else {
         rateWindow = RateWindow.FIVE_MINUTE;
      }

      failedFractionThreshold = Double.parseDouble(init.getProperty("failedFraction", "1000.0"));
      abandonedFractionThreshold = Double.parseDouble(init.getProperty("abandonedFraction", "1000.0"));
   }

   private double getRate(final Metered meter) {
      switch(rateWindow) {
         case ONE_MINUTE: return meter.getOneMinuteRate();
         case FIFTEEN_MINUTE: return meter.getFifteenMinuteRate();
         default: return meter.getFiveMinuteRate();
      }
   }

   private int callbackThreshold = 0;
   private int failedThreshold = 0;
   private int abandonedThreshold = 0;
   private RateWindow rateWindow = RateWindow.FIVE_MINUTE;
   private double failedFractionThreshold = 0.0;
   private double abandonedFractionThreshold = 0.0;
}
