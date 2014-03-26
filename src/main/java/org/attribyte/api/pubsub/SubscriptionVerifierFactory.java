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

/**
 * Creates instances of <code>SubscriptionVerifier</code>.
 */
public interface SubscriptionVerifierFactory extends MetricSet {
   
   /**
    * Creates a <code>SubscriptionVerifier</code>.
    * @param request The HTTP subscription request.
    * @param hub The hub to which the request was sent.
    * @param subscriber The subscriber.
    * @return The verifier.
    */
   public SubscriptionVerifier create(final Request request, final HubEndpoint hub, final Subscriber subscriber);
   
}
