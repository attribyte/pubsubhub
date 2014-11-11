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

package org.attribyte.api.pubsub.impl;

import org.attribyte.api.http.Request;
import org.attribyte.api.pubsub.HubEndpoint;

import java.util.Properties;

/**
 * Creates instances of the default <code>Callback</code> implementation.
 */
public class CallbackFactory implements org.attribyte.api.pubsub.CallbackFactory {

   @Override
   public Callback create(final long receiveTimestampNanos,
                          final Request request, final long subscriptionId, final int priority, final HubEndpoint hub) {
      return new Callback(receiveTimestampNanos, request, subscriptionId, priority, hub,
              hub.getGlobalCallbackMetrics(),
              hub.getHostCallbackMetrics(request.getURI().getHost()),
              hub.getSubscriptionCallbackMetrics(subscriptionId));
   }

   @Override
   public void init(final Properties props) {
   }

   @Override
   public boolean shutdown(final int waitTimeSeconds) {
      return true;
   }
}
