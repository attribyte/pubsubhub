package org.attribyte.api.pubsub;

import com.codahale.metrics.Meter;
import org.attribyte.api.InitializationException;

import java.util.Properties;

/**
 * A strategy for determining if a subscription should be disabled given
 * the count and rate(s) of failed/abandoned callbacks.
 */
public interface DisableSubscriptionStrategy {

   /**
    * A strategy that never disables a subscription.
    */
   public static final DisableSubscriptionStrategy NEVER_DISABLE = new DisableSubscriptionStrategy() {

      @Override
      public boolean disableSubscription(final Subscription subscription,
                                         final Meter callbackMeter,
                                         final Meter failedCallbackMeter, final Meter abandonedCallbackMeter) {
         return false;
      }

      @Override
      public void init(final Properties props) throws InitializationException {
      }
   };

   /**
    * Determine if a subscription should be disabled.
    * @param subscription The subscription.
    * @param callbackMeter Measurement of all callbacks.
    * @param failedCallbackMeter Measurement of failed callbacks since subscription was enabled.
    * @param abandonedCallbackMeter Measurement of abandoned callbacks since subscription was enabled.
    * @return Should the subscription be disabled?
    */
   public boolean disableSubscription(Subscription subscription,
                                      Meter callbackMeter,
                                      Meter failedCallbackMeter, Meter abandonedCallbackMeter);

   /**
    * Initialize the strategy.
    * @param props The properties.
    * @throws org.attribyte.api.InitializationException on initialization error.
    */
   public void init(Properties props) throws InitializationException;
}
