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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.HubEndpoint;

/**
 * The default callback implementation.
 * <p>
 * If the callback fails, it is queued again with reduced priority.
 * </p>
 */
public class Callback extends org.attribyte.api.pubsub.Callback {

   protected Callback(final Request request,
                      final long subscriptionId,
                      final int priority,
                      final HubEndpoint hub,
                      final Timer timer,
                      final Meter failedCallbacks,
                      final Meter abandonedCallbacks) {
      super(request, subscriptionId, priority, hub);
      this.timer = timer;
      this.failedCallbacks = failedCallbacks;
      this.abandonedCallbacks = abandonedCallbacks;
   }

   @Override
   public void run() {
      try {
         final Response response;
         final Timer.Context ctx = timer.time();
         try {
            response = hub.getHttpClient().send(request);
         } finally {
            ctx.stop();
         }
         if(!Response.Code.isOK(response.getResponseCode())) {
            failedCallbacks.mark();
            boolean enqueued = hub.enqueueFailedCallback(this);
            if(!enqueued) {
               abandonedCallbacks.mark();
            }
         }
      } catch(Throwable ioe) {
         failedCallbacks.mark();
         boolean enqueued = hub.enqueueFailedCallback(this);
         if(!enqueued) {
            abandonedCallbacks.mark();
         }
      }
   }

   private final Timer timer;
   private final Meter failedCallbacks;
   private final Meter abandonedCallbacks;
}
