package org.attribyte.api.pubsub.impl.server.util;

import org.attribyte.api.pubsub.Subscription;

public class SubscriptionVerifyRecord extends SubscriptionEvent {

   /**
    * Reports a failed subscription verification.
    * @param callbackURL The callback URL.
    * @param callbackResponseCode The response code.
    * @param reason A reason, if any.
    * @param attempts The number of attempted verifications.
    * @param abandoned Was this the last verification attempt.
    */
   public SubscriptionVerifyRecord(String callbackURL,
                                   int callbackResponseCode,
                                   String reason, int attempts,
                                   boolean abandoned) {
      this.callbackURL = callbackURL;
      this.callbackResponseCode = callbackResponseCode;
      this.reason = reason;
      this.attempts = attempts;
      this.abandoned = abandoned;
      this._isFailed = true;
   }

   public SubscriptionVerifyRecord(Subscription subscription) {
      this._isFailed = false;
      this.callbackURL = subscription.getCallbackURL();
      this.callbackResponseCode = 200;
      this.reason = null;
      this.attempts = 0;
      this.abandoned = false;
   }

   @Override
   public final String toString() {
      return null;
   }

   @Override
   public final boolean isFailed() {
      return _isFailed;
   }

   @Override
   public final boolean isVerify() {
      return true;
   }

   /**
    * @return Is the response code set?
    */
   public final boolean getCodeIsSet() {
      return callbackResponseCode > 0;
   }

   /**
    * The callback URL.
    */
   public final String callbackURL;

   /**
    * The response code received when following the callback URL.
    */
   public final int callbackResponseCode;

   /**
    * A failure reason, if any.
    */
   public final String reason;

   /**
    * The number of attempts.
    */
   public final int attempts;

   /**
    * Is this an abandoned verification?
    */
   public final boolean abandoned;

   /**
    * Is this a failed verification.
    */
   private final boolean _isFailed;
}
