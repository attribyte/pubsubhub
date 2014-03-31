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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A <code>Runnable</code> class used to process <code>Notifications</code>.
 * <p>
 * Instances are responsible for iterating through all active
 * subscriptions for the notification's <code>Topic</code> and
 * queuing the HTTP post to the callback URLs.
 * </p>
 */
public abstract class Notifier implements Runnable {

   public Notifier(final Notification notification, final HubEndpoint hub) {
      this.notification = notification;
      this.hub = hub;
   }

   /**
    * Increment the number of attempts for this notifier.
    * @return The number of attempts after the increment.
    */
   public int incrementAttempts() {
      return attempts.incrementAndGet();
   }

   protected AtomicInteger attempts = new AtomicInteger(0);
   protected final Notification notification;
   protected final HubEndpoint hub;
}
