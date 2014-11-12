/*
 * Copyright 2010, 2014 Attribyte, LLC 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.  
 * 
 */

package org.attribyte.api.pubsub;

/**
 * A <code>Runnable</code> implementation used to send subscription
 * callbacks. The <code>Comparable</code> implementation
 * compares priority to allow the possibility of a priority queue for scheduling.
 * Callback instances are <em>not</em> thread-safe.
 */
public abstract class Callback implements Runnable, Comparable<Callback> {

   @Override
   public abstract void run();

   @Override
   public int compareTo(Callback other) {
      return getPriority() == other.getPriority() ? 0 : getPriority() < other.getPriority() ? -1 : 1;
   }

   /**
    * Creates a callback.
    * @param hub The hub endpoint sending the callback.
    */
   protected Callback(final HubEndpoint hub) {
      this.hub = hub;
   }

   /**
    * Gets the subscription id associated with this callback.
    * @return The subscription id.
    */
   public abstract long getSubscriptionId();

   /**
    * Gets the subscription priority.
    * @return The priority.
    */
   public abstract int getPriority();

   /**
    * Increments the number of attempts for this callback.
    * @return The number of attempts after the increment.
    */
   public int incrementAttempts() {
      if(attempts > 0) {
         lastFailedTimestamp = System.currentTimeMillis();
      }
      return ++attempts;
   }

   /**
    * Gets the number of attempts.
    * @return The number of attempts.
    */
   public int getAttempts() {
      return attempts;
   }

   /**
    * Gets the callback create time.
    * @return The timestamp.
    */
   public long getCreateTimestamp() {
      return createTimestamp;
   }

   /**
    * Gets the time, if any, of the last failed callback attempt.
    * @return The timestamp, or <code>0</code> if never failed.
    */
   public long getLastFailedTimestamp() {
      return lastFailedTimestamp;
   }

   protected final HubEndpoint hub;

   protected int attempts;
   protected long createTimestamp = System.currentTimeMillis();
   protected long lastFailedTimestamp = 0L;
}