package org.attribyte.api.pubsub;

import com.codahale.metrics.Meter;

/**
 * Subscription-specific meters.
 */
public class SubscriptionCallbackMeters {

   /**
    * Creates a set of meters.
    * @param subscriptionId The subscription id.
    */
   SubscriptionCallbackMeters(final long subscriptionId) {
      this.subscriptionId = subscriptionId;
   }

   /**
    * The subscription id.
    */
   public final long subscriptionId;

   /**
    * A meter that tracks all callbacks.
    */
   public final Meter callbackMeter = new Meter();

   /**
    * A meter that tracks failed callbacks.
    */
   public final Meter failedCallbackMeter = new Meter();

   /**
    * A meter that tracks abandoned callbacks.
    */
   public final Meter abandonedCallbackMeter = new Meter();
}
