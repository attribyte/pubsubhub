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
import org.attribyte.api.pubsub.CallbackMetrics;
import org.attribyte.api.pubsub.HubEndpoint;

import java.util.concurrent.TimeUnit;

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
                      final CallbackMetrics globalMetrics,
                      final CallbackMetrics hostMetrics,
                      final CallbackMetrics subscriptionMetrics) {
      super(request, subscriptionId, priority, hub);
      this.globalMetrics = globalMetrics;
      this.hostMetrics = hostMetrics;
      this.subscriptionMetrics = subscriptionMetrics;
   }

   @Override
   public void run() {
      try {
         final Response response;
         final Timer.Context ctx = globalMetrics != null ? globalMetrics.callbacks.time() : null;
         try {
            response = hub.getHttpClient().send(request);
         } finally {
            if(ctx != null) recordTime(ctx.stop());
         }


         System.out.println("callback got status code " + response.getStatusCode());

         if(!Response.Code.isOK(response.getStatusCode())) {
            markFailed();
            boolean enqueued = hub.enqueueFailedCallback(this);
            if(!enqueued) {
               markAbandoned();
            }
         }
      } catch(Error e) {
         e.printStackTrace();
         throw e;
      } catch(Throwable ioe) {
         System.err.println(ioe.getMessage());
         ioe.printStackTrace();
         markFailed();
         boolean enqueued = hub.enqueueFailedCallback(this);
         if(!enqueued) {
            markAbandoned();
         }
      }
   }

   private void recordTime(final long nanos) {
      if(hostMetrics != null) hostMetrics.callbacks.update(nanos, TimeUnit.NANOSECONDS);
      if(subscriptionMetrics != null) subscriptionMetrics.callbacks.update(nanos, TimeUnit.NANOSECONDS);
   }

   private void markFailed() {
      if(globalMetrics != null) globalMetrics.failedCallbacks.mark();
      if(hostMetrics != null) hostMetrics.failedCallbacks.mark();
      if(subscriptionMetrics != null) subscriptionMetrics.failedCallbacks.mark();
   }

   private void markAbandoned() {
      if(globalMetrics != null) globalMetrics.abandonedCallbacks.mark();
      if(hostMetrics != null) hostMetrics.abandonedCallbacks.mark();
      if(subscriptionMetrics != null) subscriptionMetrics.abandonedCallbacks.mark();
   }

   private final CallbackMetrics globalMetrics;
   private final CallbackMetrics hostMetrics;
   private final CallbackMetrics subscriptionMetrics;

}
