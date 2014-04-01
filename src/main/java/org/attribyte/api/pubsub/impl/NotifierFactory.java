/*
 * Copyright 2010 Attribyte, LLC 
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

package org.attribyte.api.pubsub.impl;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;

import java.util.Map;
import java.util.Properties;

public class NotifierFactory implements org.attribyte.api.pubsub.NotifierFactory {

   @Override
   public Notifier create(final Notification notification, final HubEndpoint hub) {
      return new Notifier(notification, hub, notificationTimer);
   }

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.<String, Metric>of(
              "notifications", notificationTimer
      );
   }

   @Override
   public void init(final Properties props) {
   }

   @Override
   public boolean shutdown(final int waitTimeSeconds) {
      return true;
   }

   /**
    * Measures the notification rate and the time required to
    * select subscriptions and enqueue callbacks.
    */
   static final Timer notificationTimer = new Timer();
}
