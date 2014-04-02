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
import com.google.common.util.concurrent.ListeningExecutorService;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.HubEndpoint;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.api.Response.CompleteListener;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.client.util.BytesContentProvider;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * The default callback implementation.
 * <p>
 * If the callback fails, it is queued again with reduced priority.
 * </p>
 */
public class AsyncCallback extends org.attribyte.api.pubsub.Callback {

   protected AsyncCallback(final Request request,
                           final long subscriptionId,
                           final int priority,
                           final HubEndpoint hub,
                           final Timer timer,
                           final Meter failedCallbacks,
                           final Meter abandonedCallbacks,
                           final HttpClient httpClient) {
      super(request, subscriptionId, priority, hub);
      this.timer = timer;
      this.failedCallbacks = failedCallbacks;
      this.abandonedCallbacks = abandonedCallbacks;
      this.httpClient = httpClient;
   }

   @Override
   public void run() {
      final Timer.Context ctx = timer.time();
      httpClient.POST(joinURL(request))
              .timeout(5L, TimeUnit.SECONDS)  //TODO
              .followRedirects(false)
              .content(new ByteBufferContentProvider(request.getBody()))
              .send(new CompleteListener() {
                 @Override
                 public void onComplete(final Result result) {
                    ctx.stop();
                    if(!result.isSucceeded() || Response.Code.isOK(result.getResponse().getStatus())) {
                       failedCallbacks.mark();
                       boolean enqueued = hub.enqueueFailedCallback(AsyncCallback.this);
                       if(!enqueued) {
                          abandonedCallbacks.mark();
                       }

                       //TODO: Log...
                    }
                 }
              });
   }


   private String joinURL(final Request request) {
      StringBuilder urlBuf = request.getRequestURL();
      if(request.getQueryString() != null) {
         urlBuf.append("?").append(request.getQueryString());
      }
      return urlBuf.toString();
   }


   private final HttpClient httpClient;
   private final Timer timer;
   private final Meter failedCallbacks;
   private final Meter abandonedCallbacks;
}
