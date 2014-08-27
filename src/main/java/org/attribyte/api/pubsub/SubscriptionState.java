package org.attribyte.api.pubsub;

import java.util.Date;

/**
 * A representation of the state of (topic) subscriptions. Used
 * to determine if cached subscriptions are valid, etc.
 */
public interface SubscriptionState {

   /**
    * Are the two states equal?
    * @param other The other state.
    * @return Are they equal.
    */
   public boolean isEqual(SubscriptionState other);

   /**
    * The time any subscriptions were last modified.
    * @return The last modified time.
    */
   public Date lastModifiedTime();
}
