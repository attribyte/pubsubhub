package org.attribyte.api.pubsub.impl.server.util;

import java.util.Date;
import java.util.List;

public abstract class SubscriptionEvent implements Comparable<SubscriptionEvent> {

   /**
    * An interface that allows recent subscription requests to be retrieved.
    */
   public static interface Source {

      /**
       * Gets the latest subscription requests.
       * @param limit The maximum returned.
       * @return The list of subscription requests.
       */
      public List<SubscriptionEvent> latestEvents(int limit);
   }

   @Override
   public final int compareTo(SubscriptionEvent other) {
      //Note...sort in descending order...
      return -1 * createTime.compareTo(other.createTime);
   }

   /**
    * The time when request was received.
    */
   public final Date createTime = new Date();

   /**
    * Does this event represent failure?
    */
   public abstract boolean isFailed();

   @Override
   public abstract String toString();

   /**
    * Is the event a verify event?
    */
   public abstract boolean isVerify();
}
