package org.attribyte.api.pubsub;

import java.util.Properties;

/**
 * Allows custom retry strategy like exponentiall backoff.
 */
public interface RetryStrategy {

   /**
    * Implements exponential backoff.
    */
   public static class ExponentialBackoff implements RetryStrategy {

      /**
       * Creates exponential backoff with a maximum of 14 attempts
       * and delay interval of 100ms (unless subsequently initialized with properties).
       */
      public ExponentialBackoff() {
         this.maxAttempts = 14;
         this.delayIntervalMillis = 100L;
      }

      /**
       * Creates exponential backoff with specified maximum attempts and
       * delay interval.
       * @param maxAttempts The maximum number of attempts.
       * @param delayIntervalMillis The delay interval in milliseconds.
       */
      public ExponentialBackoff(final int maxAttempts, final long delayIntervalMillis) {
         this.maxAttempts = maxAttempts;
         this.delayIntervalMillis = delayIntervalMillis;
      }

      public long backoffMillis(final int numAttempts) {
         return numAttempts < maxAttempts ? ((long)Math.pow(2, numAttempts) * delayIntervalMillis) : -1L;
      }

      private final int maxAttempts;
      private final long delayIntervalMillis;
   }

   /**
    * Specifies the number of milliseconds before retry is attempted.
    * @param numAttempts The current number of attempts.
    * @return The number of milliseconds. If less than zero, retry will not be attempted.
    */
   public long backoffMillis(final int numAttempts);
}
