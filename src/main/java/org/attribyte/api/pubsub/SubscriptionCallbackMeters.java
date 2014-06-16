package org.attribyte.api.pubsub;

import com.codahale.metrics.Meter;

/**
 * Subscription-specific meters.
 */
public class SubscriptionCallbackMeters {

   SubscriptionCallbackMeters(final long subscriptionId) {
      this.subscriptionId = subscriptionId;
   }

   public final long subscriptionId;
   public final Meter callbackMeter = new Meter();
   public final Meter failedCallbackMeter = new Meter();
   public final Meter abandonedCallbackMeter = new Meter();
}
