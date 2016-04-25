/*
 * Copyright 2014, 2016 Attribyte, LLC
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

package org.attribyte.api.pubsub.impl.client;

import com.google.common.base.Optional;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sends subscription requests to a hub.
 */
public class SubscriptionClient {

   /**
    * Creates the client.
    */
   public SubscriptionClient() {
      SslContextFactory sslContextFactory = new SslContextFactory();
      this.httpClient = new HttpClient(sslContextFactory);
      this.httpClient.setFollowRedirects(false);
      this.httpClient.setConnectTimeout(10000L);
      this.httpClient.setCookieStore(new HttpCookieStore.Empty());
   }

   /**
    * The single instance of accepted subscription result. New instances of result will only be
    * created on errors.
    */
   private static final Result ACCEPTED_RESULT = new Result(HttpStatus.ACCEPTED_202, "", null);

   /**
    * A subscription request result.
    */
   public static class Result {

      Result(final int code, final String message, final Throwable cause) {
         this.code = code;
         this.message = message != null ? Optional.of(message) : Optional.<String>absent();
         this.cause = cause != null ? Optional.of(cause) : Optional.<Throwable>absent();
         this.isError = code != HttpStatus.ACCEPTED_202;
      }

      /**
       * The HTTP result from the hub.
       */
      public final int code;

      /**
       * Was the result an error?
       */
      public final boolean isError;

      /**
       * An optional message.
       */
      public final Optional<String> message;

      /**
       * An optional error cause.
       */
      public final Optional<Throwable> cause;
   }

   /**
    * Starts the client.
    * <p>
    * Must be called before use.
    * </p>
    * @throws Exception on start error.
    */
   public void start() throws Exception {
      this.httpClient.start();
   }

   /**
    * Shutdown the client.
    * @throws Exception on shutdown error.
    */
   public void shutdown() throws Exception {
      this.httpClient.stop();
   }

   /**
    * Synchronously posts a subscription request.
    * @param topicURL The subscription URL of the topic.
    * @param hubURL The hub.
    * @param callbackURL The callback for the hub.
    * @param leaseSeconds The subscription lease seconds.
    * @param hubAuth optional HTTP 'Basic' auth for the hub.
    * @return The result.
    */
   public Result postSubscriptionRequest(final String topicURL,
                                         final String hubURL,
                                         final String callbackURL,
                                         final int leaseSeconds,
                                         final Optional<BasicAuth> hubAuth) {
      return postSubscriptionRequest(topicURL, hubURL, callbackURL, leaseSeconds, Optional.<BasicAuth>absent(), hubAuth);
   }

   /**
    * Synchronously posts a subscription request with non-standard callback-auth.
    * @param topicURL The subscription URL of the topic.
    * @param hubURL The hub.
    * @param callbackURL The callback for the hub.
    * @param leaseSeconds The subscription lease seconds.
    * @param callbackAuth optional HTTP 'Basic' auth for the callback (non-standard).
    * @param hubAuth optional HTTP 'Basic' auth for the hub.
    * @return The result.
    */
   public Result postSubscriptionRequest(final String topicURL,
                                         final String hubURL,
                                         final String callbackURL,
                                         final int leaseSeconds,
                                         final Optional<BasicAuth> callbackAuth,
                                         final Optional<BasicAuth> hubAuth) {
      try {

         final Fields fields = new Fields();
         fields.add("hub.callback", callbackURL);
         fields.add("hub.mode", "subscribe");
         fields.add("hub.topic", topicURL);
         fields.add("hub.lease_seconds", Integer.toString(leaseSeconds));

         //Non-standard...
         if(callbackAuth.isPresent()) {
            fields.add("hub.x-callback_auth_scheme", "Basic");
            fields.add("hub.x-callback_auth", callbackAuth.get().headerValue.substring("Basic ".length()));
         }

         final Request request = httpClient.POST(hubURL)
                 .content(new FormContentProvider(fields))
                 .timeout(10L, TimeUnit.SECONDS);

         if(hubAuth.isPresent()) {
            request.header(BasicAuth.AUTH_HEADER_NAME, hubAuth.get().headerValue);
         }

         ContentResponse response = request.send();
         int status = response.getStatus();
         if(status == HttpStatus.ACCEPTED_202) {
            return ACCEPTED_RESULT;
         } else {
            String content = response.getContentAsString();
            return new Result(status, content, null);
         }
      } catch(InterruptedException ie) {
         Thread.currentThread().interrupt();
         return new Result(0, "Interrupted while sending", ie);
      } catch(TimeoutException te) {
         return new Result(0, "Timeout while sending", te);
      } catch(ExecutionException ee) {
         if(ee.getCause() != null) {
            return new Result(0, "Problem sending", ee.getCause());
         } else {
            return new Result(0, "Problem sending", ee);
         }
      }
   }

   /**
    * Synchronously posts an unsubscribe request.
    * @param topicURL The subscription URL of the topic.
    * @param hubURL The hub.
    * @param callbackURL The callback for the hub.
    * @param hubAuth optional HTTP 'Basic' auth for the hub.
    * @return The result.
    */
   public Result postUnsubscribeRequest(final String topicURL,
                                        final String hubURL,
                                        final String callbackURL,
                                        final Optional<BasicAuth> callbackAuth,
                                        final Optional<BasicAuth> hubAuth) {
      try {

         final Fields fields = new Fields();
         fields.add("hub.callback", callbackURL);
         fields.add("hub.mode", "unsubscribe");
         fields.add("hub.topic", topicURL);

         //Non-standard...
         if(callbackAuth.isPresent()) {
            fields.add("hub.x-callback_auth_scheme", "Basic");
            fields.add("hub.x-callback_auth", callbackAuth.get().headerValue.substring("Basic ".length()));
         }

         final Request request = httpClient.POST(hubURL)
                 .content(new FormContentProvider(fields))
                 .timeout(10L, TimeUnit.SECONDS);

         if(hubAuth.isPresent()) {
            request.header(BasicAuth.AUTH_HEADER_NAME, hubAuth.get().headerValue);
         }

         ContentResponse response = request.send();
         int status = response.getStatus();
         if(status == HttpStatus.ACCEPTED_202) {
            return ACCEPTED_RESULT;
         } else {
            String content = response.getContentAsString();
            return new Result(status, content, null);
         }
      } catch(InterruptedException ie) {
         Thread.currentThread().interrupt();
         return new Result(0, "Interrupted while sending", ie);
      } catch(TimeoutException te) {
         return new Result(0, "Timeout while sending", te);
      } catch(ExecutionException ee) {
         if(ee.getCause() != null) {
            return new Result(0, "Problem sending", ee.getCause());
         } else {
            return new Result(0, "Problem sending", ee);
         }
      }
   }

   private final HttpClient httpClient;
}