package org.attribyte.api.pubsub.impl.client;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.*;
import org.attribyte.api.http.Response;
import org.attribyte.api.pubsub.Notification;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.util.HttpCookieStore;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Asynchronously pushes notifications to hubs.
 */
public class AsyncPublisher implements MetricSet {

   /**
    * A notification publish result.
    */
   public static final class Result {

      Result(final int code, final String message, final Throwable cause, final Notification notification) {
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
    * Creates a publisher with an unbounded notification queue and
    * 30s notification timeout.
    * @param numProcessors The number of threads processing the notification queue.
    * @throws Exception on initialization error.
    */
   public AsyncPublisher(final int numProcessors) throws Exception {
      this(numProcessors, 0, 30);
   }

   /**
    * Creates a publisher with a specified notification queue size.
    * @param numProcessors The number of threads processing the notification queue.
    * @param maxQueueSize The maximum queue size. If < 1, notification queue is unbounded.
    * @param notificationTimeoutSeconds The notification send timeout.
    * @throws Exception on initialization error.
    */
   public AsyncPublisher(final int numProcessors, final int maxQueueSize, final int notificationTimeoutSeconds)
           throws Exception {
      final BlockingQueue<Runnable> notifications;
      assert(numProcessors > 0);
      if(maxQueueSize > 0) {
         notifications = new ArrayBlockingQueue<Runnable>(maxQueueSize);
      } else {
         notifications = new LinkedBlockingQueue<Runnable>();
      }


      ThreadPoolExecutor executor = new ThreadPoolExecutor(numProcessors, numProcessors, 0L, TimeUnit.MILLISECONDS,
              notifications, new ThreadFactoryBuilder().setNameFormat("async-publisher-%d").build());
      executor.prestartAllCoreThreads();
      this.notificationExecutor = MoreExecutors.listeningDecorator(executor);
      this.notificationQueueSize = new CachedGauge<Integer>(15L, TimeUnit.SECONDS) {
         protected Integer loadValue() { return notifications.size(); }
      };
      SslContextFactory sslContextFactory = new SslContextFactory();
      this.httpClient = new HttpClient(sslContextFactory);
      this.httpClient.setFollowRedirects(false);
      this.httpClient.setConnectTimeout(10000L);
      this.httpClient.setCookieStore(new HttpCookieStore.Empty());
      this.notificationTimeoutSeconds = notificationTimeoutSeconds;
   }

   /**
    * Enqueue a notification for future posting to the hub.
    * @param notification The notification.
    * @param auth The optional HTTP 'Basic' auth.
    * @return The (listenable) future result.
    */
   public final ListenableFuture<Result> enqueueNotification(final Notification notification, final Optional<BasicAuth> auth) {
      try {
         return notificationExecutor.submit(new NotificationCallable(notification, auth));
      } catch(RejectedExecutionException re) {
         return Futures.immediateFailedFuture(re);
      }
   }

   /**
    * A callable for notifications.
    */
   private final class NotificationCallable implements Callable<Result> {

      NotificationCallable(final Notification notification, final Optional<BasicAuth> auth) {
         this.notification = notification;
         this.auth = auth;
      }

      public Result call() {
         return postNotification(notification, auth);
      }

      private final Notification notification;
      private final Optional<BasicAuth> auth;
   }

   private Result postNotification(final Notification notification, final Optional<BasicAuth> auth) {

      Timer.Context ctx = notificationSendTime.time();

      try {
         ContentResponse response = auth.isPresent() ?
                 httpClient.POST(notification.getTopic().getURL())
                         .content(new ByteBufferContentProvider(notification.getContent()))
                         .timeout(notificationTimeoutSeconds, TimeUnit.SECONDS)
                         .header(BasicAuth.AUTH_HEADER_NAME, auth.get().headerValue)
                         .send() :
                 httpClient.POST(notification.getTopic().getURL())
                         .content(new ByteBufferContentProvider(notification.getContent()))
                         .timeout(notificationTimeoutSeconds, TimeUnit.SECONDS)
                         .send();

         int code = response.getStatus();
         if(code == Response.Code.ACCEPTED) {
            return ACCEPTED_RESULT;
         } else {
            String message = response.getContentAsString();
            return new Result(code, message, null, notification);
         }
      } catch(InterruptedException ie) {
         Thread.currentThread().interrupt();
         return new Result(0, "Interrupted while sending", ie, notification);
      } catch(TimeoutException te) {
         return new Result(0, "Timeout while sending", te, notification);
      } catch(ExecutionException ee) {
         if(ee.getCause() != null) {
            return new Result(0, "Problem sending", ee.getCause(), notification);
         } else {
            return new Result(0, "Problem sending", ee, notification);
         }
      } catch(Throwable t) {
         t.printStackTrace();
         return new Result(0, "Internal error", t, notification);
      } finally {
         ctx.stop();
      }
   }

   /**
    * Starts the publisher.
    * <p>
    *    Must be called before use.
    * </p>
    * @throws Exception on start error.
    */
   public void start() throws Exception {
      this.httpClient.start();
   }

   /**
    * Shutdown the publisher.
    * @param maxWaitSeconds The maximum amount of time to wait for in-process notifications to complete.
    * @throws Exception on shutdown error.
    */
   public void shutdown(int maxWaitSeconds) throws Exception {
      this.notificationExecutor.shutdown();
      this.notificationExecutor.awaitTermination(maxWaitSeconds , TimeUnit.SECONDS);
      if(!this.notificationExecutor.isShutdown()) {
         this.notificationExecutor.shutdownNow();
      }
      this.httpClient.stop();
   }

   /**
    * Gets metrics for registration.
    * @return The metrics.
    */
   public Map<String, Metric> getMetrics() {
      ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
      builder.put("notification-queue-size", notificationQueueSize);
      builder.put("notification-send-time", notificationSendTime);
      return builder.build();
   }

   /**
    * The single instance of accepted result. New instances of result will only be
    * created on errors.
    */
   private static final Result ACCEPTED_RESULT = new Result(Response.Code.ACCEPTED, "", null, null);

   /**
    * The executor handling notifications.
    */
   private final ListeningExecutorService notificationExecutor;

   /**
    * The (cached) size of the notification queue.
    */
   private final CachedGauge<Integer> notificationQueueSize;

   /**
    * Measures timing for notification send.
    */
   private final Timer notificationSendTime = new Timer();

   /**
    * The notification send timeout.
    */
   private final int notificationTimeoutSeconds;

   /**
    * The HTTP client sending notifications.
    */
   private final HttpClient httpClient;
}
