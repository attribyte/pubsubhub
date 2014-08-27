package org.attribyte.api.pubsub.impl;

import java.util.Date;

/**
 * The subscription state.
 *
 * <p>
 * Because the modified timestamp resolution is just
 * one second, it is possible, though quite unlikely that a race
 * condition could return an equal subscription state while, in fact,
 * there have been modifications. If this isn't acceptable,
 * other implementations of this interface are possible.
 * For example, auto-increment could be used to track every
 * subscription modification.
 * </p>
 */
public class SubscriptionState implements org.attribyte.api.pubsub.SubscriptionState {

   /**
    * Creates the state.
    * @param timestampMillis The last modified timestamp.
    */
   SubscriptionState(final long timestampMillis) {
      this.timestampMillis = timestampMillis;
   }

   @Override
   public boolean isEqual(org.attribyte.api.pubsub.SubscriptionState other) {
      return other instanceof SubscriptionState && timestampMillis == ((SubscriptionState)other).timestampMillis;
   }

   @Override
   public Date lastModifiedTime() {
      return null;
   }

   private final long timestampMillis;
}
