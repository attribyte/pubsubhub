/*
 * Copyright 2014, 2016, 2018 Attribyte, LLC
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

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import org.attribyte.api.InitializationException;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.http.Client;
import org.attribyte.api.http.ClientOptions;
import org.attribyte.api.http.FormPostRequestBuilder;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.impl.jetty.JettyClient;
import org.eclipse.jetty.http.HttpStatus;

import java.io.IOException;

/**
 * Sends subscription requests to a hub.
 */
public class SubscriptionClient {

   /**
    * Creates a subscription client with defaults.
    * @throws InitializationException on failure to created default HTTP client.
    */
   public SubscriptionClient() throws InitializationException {
      this(new JettyClient(ClientOptions.IMPLEMENTATION_DEFAULT));
   }

   /**
    * Creates a subscription client with a pre-configured HTTP client.
    * @param client The HTTP client.
    */
   public SubscriptionClient(final Client client) {
      this.httpClient = client;
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

      /**
       * Throws an exception if this result is an error.
       * @throws Exception if the result is an error.
       */
      public void throwExceptionIfError() throws Exception {
         if(isError) {
            String message = Integer.toString(code);
            if(this.message.isPresent()) {
               message = message + " - " + this.message.get();
            }
            throw cause.isPresent() ? new Exception(message, cause.get()) : new Exception(message);
         }
      }

      @Override
      public String toString() {
         return MoreObjects.toStringHelper(this)
                 .add("code", code)
                 .add("isError", isError)
                 .add("message", message)
                 .add("cause", cause)
                 .toString();
      }
   }

   /**
    * Starts the client.
    * <p>
    * Must be called before use.
    * </p>
    * @throws Exception on start error.
    */
   public void start() throws Exception {
   }

   /**
    * Shutdown the client.
    * @throws Exception on shutdown error.
    */
   public void shutdown() throws Exception {
      this.httpClient.shutdown();
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

         FormPostRequestBuilder requestBuilder = new FormPostRequestBuilder(hubURL);

         requestBuilder.addParameter("hub.callback", callbackURL);
         requestBuilder.addParameter("hub.mode", "subscribe");
         requestBuilder.addParameter("hub.topic", topicURL);
         requestBuilder.addParameter("hub.lease_seconds", Integer.toString(leaseSeconds));

         //Non-standard...
         if(callbackAuth.isPresent()) {
            requestBuilder.addParameter("hub.x-callback_auth_scheme", "Basic");
            requestBuilder.addParameter("hub.x-callback_auth", callbackAuth.get().headerValue.substring("Basic ".length()));
         }

         if(hubAuth.isPresent()) {
            requestBuilder.addHeader(BasicAuth.AUTH_HEADER_NAME, hubAuth.get().headerValue);
         }

         Response response = httpClient.send(requestBuilder.create());
         return response.statusCode == HttpStatus.ACCEPTED_202 ?
                 ACCEPTED_RESULT : new Result(response.statusCode, response.getBody().toStringUtf8(), null);
      } catch(InvalidURIException iue) {
         return new Result(0, "Hub URL is invalid", iue);
      } catch(IOException ioe) {
         return new Result(0, "Communication failure", ioe);
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

         FormPostRequestBuilder requestBuilder = new FormPostRequestBuilder(hubURL);


         requestBuilder.addHeader("hub.callback", callbackURL);
         requestBuilder.addHeader("hub.mode", "unsubscribe");
         requestBuilder.addHeader("hub.topic", topicURL);

         //Non-standard...
         if(callbackAuth.isPresent()) {
            requestBuilder.addHeader("hub.x-callback_auth_scheme", "Basic");
            requestBuilder.addHeader("hub.x-callback_auth", callbackAuth.get().headerValue.substring("Basic ".length()));
         }

         if(hubAuth.isPresent()) {
            requestBuilder.addHeader(BasicAuth.AUTH_HEADER_NAME, hubAuth.get().headerValue);
         }

         Response response = httpClient.send(requestBuilder.create());
         return response.statusCode == HttpStatus.ACCEPTED_202 ?
                 ACCEPTED_RESULT : new Result(response.statusCode, response.getBody().toStringUtf8(), null);
      } catch(InvalidURIException iue) {
         return new Result(0, "Hub URL is invalid", iue);
      } catch(IOException ioe) {
         return new Result(0, "Communication failure", ioe);
      }
   }

   private final Client httpClient;
}