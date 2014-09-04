package org.attribyte.api.pubsub;

/**
 * Callback metrics for a specific host.
 */
public class HostCallbackMetrics extends CallbackMetrics {

   /**
    * Creates callback metrics for a host.
    * @param host The host.
    */
   public HostCallbackMetrics(final String host) {
      this.host = host;
   }

   public final String host;
}