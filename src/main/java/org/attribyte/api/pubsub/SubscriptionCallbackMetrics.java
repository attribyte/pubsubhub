package org.attribyte.api.pubsub;

/**
 * Callback metrics for a subscription identified by id.
 */
public class SubscriptionCallbackMetrics extends CallbackMetrics {

   /**
    * Creates callback metrics for a subscription.
    * @param subscriptionId The subscription id.
    */
   public SubscriptionCallbackMetrics(final long subscriptionId) {
      this.subscriptionId = subscriptionId;
   }

   public final long subscriptionId;
}
