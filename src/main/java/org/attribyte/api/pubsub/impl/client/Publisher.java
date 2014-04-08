package org.attribyte.api.pubsub.impl.client;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.Notification;

import java.util.Map;

/**
 * Asynchronously pushes notifications to hubs.
 */
public interface Publisher extends MetricSet {

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
    * The single instance of accepted result. New instances of result will only be
    * created on errors.
    */
   static final NotificationResult ACCEPTED_RESULT = new NotificationResult(Response.Code.ACCEPTED, "", null, null);
}
