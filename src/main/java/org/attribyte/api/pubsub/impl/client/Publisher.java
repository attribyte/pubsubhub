/*
 * Copyright 2014 Attribyte, LLC
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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import java.util.Map;

/**
 * Asynchronously pushes notifications to hubs.
 */
public interface Publisher extends MetricSet {

   /**
    * A notification sent to a hub to be broadcast
    * to all subscribers.
    */
   public static class Notification {

      /**
       * Creates a notification.
       * @param url The hub URL that will accept the notification.
       * @param content The content.
       */
      public Notification(final String url, final ByteString content) {
         this.url = url;
         this.content = content;
      }

      /**
       * The hub URL that accepts the notification for broadcast.
       */
      public final String url;

      /**
       * The notification content.
       */
      public final ByteString content;

      @Override
      public String toString() {
         return MoreObjects.toStringHelper(this)
                 .add("url", url)
                 .add("content", content)
                 .toString();
      }
   }

   /**
    * A notification publish result.
    */
   public static final class NotificationResult {

      NotificationResult(final int code, final String message, final Throwable cause, final Notification notification) {
         this.code = code;
         this.message = message != null ? Optional.of(message) : Optional.<String>absent();
         this.cause = cause != null ? Optional.of(cause) : Optional.<Throwable>absent();
         this.isError = !(code > 199 && code < 300);
         this.notification = notification != null ? Optional.of(notification) : Optional.<Notification>absent();
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
       * The optionally included notification associated with the (error) result.
       */
      public final Optional<Notification> notification;

      @Override
      public String toString() {
         return MoreObjects.toStringHelper(this)
                 .add("code", code)
                 .add("isError", isError)
                 .add("message", message)
                 .add("cause", cause)
                 .add("notification", notification)
                 .toString();
      }
   }

   /**
    * Enqueue a notification for future posting to the hub.
    * @param notification The notification.
    * @param auth The optional HTTP 'Basic' auth.
    * @return The (listenable) future result.
    */
   public ListenableFuture<NotificationResult> enqueueNotification(Notification notification, Optional<BasicAuth> auth);

   /**
    * Starts the publisher.
    * <p>
    * Must be called before use.
    * </p>
    * @throws Exception on start error.
    */
   public void start() throws Exception;

   /**
    * Shutdown the publisher.
    * @param maxWaitSeconds The maximum amount of time to wait for in-process notifications to complete.
    * @throws Exception on shutdown error.
    */
   public void shutdown(int maxWaitSeconds) throws Exception;

   /**
    * Gets metrics for registration.
    * @return The metrics.
    */
   public Map<String, Metric> getMetrics();

   /**
    * The HTTP accepted code (202).
    */
   static final int HTTP_ACCEPTED = 202;

   /**
    * The single instance of accepted result. New instances of result will only be
    * created on errors.
    */
   static final NotificationResult ACCEPTED_RESULT = new NotificationResult(HTTP_ACCEPTED, "", null, null);
}
