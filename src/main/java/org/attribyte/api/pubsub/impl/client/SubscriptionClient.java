package org.attribyte.api.pubsub.impl.client;

import com.google.common.base.Optional;
import org.attribyte.api.http.Response;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
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
   public SubscriptionClient() throws Exception {
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
   private static final Result ACCEPTED_RESULT = new Result(Response.Code.ACCEPTED, "", null);

   /**
    * A subscription request result.
    */
   public static class Result {

      Result(final int code, final String message, final Throwable cause) {
         this.code = code;
         this.message = message != null ? Optional.of(message) : Optional.<String>absent();
         this.cause = cause != null ? Optional.of(cause) : Optional.<Throwable>absent();
         this.isError = code != Response.Code.ACCEPTED;
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
    *    Must be called before use.
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
    * @param hubURL The hub to which we are subscribing.
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
    * @param hubURL The hub to which we are subscribing.
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

         final Request request = httpClient.POST(hubURL)
                 .param("hub.callback", callbackURL)
                 .param("hub.mode", "subscribe")
                 .param("hub.topic", topicURL)
                 .param("hub.lease_seconds", Integer.toString(leaseSeconds))
                 .timeout(10L, TimeUnit.SECONDS);

         if(hubAuth.isPresent()) {
            request.header(BasicAuth.AUTH_HEADER_NAME, hubAuth.get().headerValue);
         }

         //Non-standard...
         if(callbackAuth.isPresent()) {
            request.param("hub.x-callback_auth_scheme", "Basic");
            request.param("hub.x-callback_auth", callbackAuth.get().headerValue.substring("Basic ".length()));
         }

         ContentResponse response = request.send();
         int status = response.getStatus();
         if(status == Response.Code.ACCEPTED) {
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
