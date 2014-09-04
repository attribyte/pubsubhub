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

import com.codahale.metrics.MetricSet;
import org.attribyte.api.http.Request;

import java.util.Properties;

/**
 * Creates instances of <code>Callback</code>.
 */
public interface CallbackFactory {

   /**
    * Creates a callback.
    * @param request The HTTP request to be sent to the client.
    * @param subscriptionId The id of the subscription.
    * @param priority The callback priority.
    * @param hub The hub sending the callback.
    * @return The new callback.
    */
   public Callback create(final Request request, final long subscriptionId, final int priority, final HubEndpoint hub);

   /**
    * Allow initialization for the factory.
    * @param props The properties.
    */
   public void init(final Properties props);

   /**
    * Shutdown the factory.
    * @param waitTimeSeconds The maximum amount of time to wait for shutdown to complete.
    * @return Was shutdown complete?
    */
   public boolean shutdown(final int waitTimeSeconds);
}
