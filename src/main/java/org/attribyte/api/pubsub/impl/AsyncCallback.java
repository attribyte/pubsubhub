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
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.HubEndpoint;
import org.attribyte.api.pubsub.Notification;
import org.attribyte.api.pubsub.impl.client.BasicAuth;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * The default callback implementation.
 * <p>
 * If the callback fails, it is queued again with reduced priority.
 * </p>
 */
public class AsyncCallback extends org.attribyte.api.pubsub.Callback {

   private static final class Result {

      Result(final Request request,
             final Response response) {
         this.request = request;
         this.response = response;
         this.error = null;
      }

      Result(final Request request,
             final Throwable error) {
         this.request = request;
         this.response = null;
         this.error = error;
      }

      final Request request;
      final Response response;
      final Throwable error;
   }


   /**
    * Enables async callback.
    */
   private final class CallbackCallable implements Callable<Result> {

      CallbackCallable(final Request request) {
         this.request = request;
      }

      public Result call() {
         try {
            final Response response;
            final Timer.Context ctx = timer.time();
            try {
               response = hub.getHttpClient().send(request);
               return new Result(request, response);
            } finally {
               ctx.stop();
            }
         } catch(Throwable t) {
            return new Result(request, t);
         }
      }

      private final Request request;
   }

   /**
    * Handles the completion of an async callback by queueing failures for retry.
    */
   private final FutureCallback<Result> resultHandler = new FutureCallback<Result>() {

      @Override
      public void onSuccess(final Result result) {
         if(result.response != null && !Response.Code.isOK(result.response.getResponseCode())) {
            failedCallbacks.mark();
            boolean enqueued = hub.enqueueFailedCallback(AsyncCallback.this);
            if(!enqueued) {
               abandonedCallbacks.mark();
            }
         } else if(result.error != null) {
            failedCallbacks.mark();
            boolean enqueued = hub.enqueueFailedCallback(AsyncCallback.this);
            if(!enqueued) {
               abandonedCallbacks.mark();
            }
         }
      }

      @Override
      public void onFailure(final Throwable t) {
         failedCallbacks.mark();
         boolean enqueued = hub.enqueueFailedCallback(AsyncCallback.this);
         if(!enqueued) {
            abandonedCallbacks.mark();
         }
      }
   };

   protected AsyncCallback(final Request request,
                           final long subscriptionId,
                           final int priority,
                           final HubEndpoint hub,
                           final Timer timer,
                           final Meter failedCallbacks,
                           final Meter abandonedCallbacks,
                           final ListeningExecutorService callbackExecutor) {
      super(request, subscriptionId, priority, hub);
      this.timer = timer;
      this.failedCallbacks = failedCallbacks;
      this.abandonedCallbacks = abandonedCallbacks;
      this.callbackExecutor = callbackExecutor;
   }

   @Override
   public void run() {
      ListenableFuture<Result> futureResult = enqueueCallback(request);
      Futures.addCallback(futureResult, resultHandler, MoreExecutors.sameThreadExecutor());
   }

   /**
    * Enqueue a notification for future posting to the hub.
    * @return The (listenable) future result.
    */
   public final ListenableFuture<Result> enqueueCallback(final Request request) {
      try {
         return callbackExecutor.submit(new CallbackCallable(request));
      } catch(RejectedExecutionException re) {
         return Futures.immediateFailedFuture(re);
      }
   }

   private final ListeningExecutorService callbackExecutor;
   private final Timer timer;
   private final Meter failedCallbacks;
   private final Meter abandonedCallbacks;
}
