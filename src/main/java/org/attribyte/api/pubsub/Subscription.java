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

package org.attribyte.api.pubsub;

import com.google.common.base.Strings;
import org.attribyte.util.StringUtil;

import java.util.Date;

/**
 * A subscription.
 * @author Attribyte, LLC
 */
public class Subscription {

   /**
    * Use to create immutable instances without constructor.
    */
   public static class Builder {

      /**
       * Creates a subscription builder with a known topic and endpoint id.
       * @param id The id.
       * @param callbackURL The callback URL.
       * @param topic The topic.
       * @param endpointId The endpoint id.
       */
      public Builder(final long id, final String callbackURL, final Topic topic, final long endpointId) {
         this.id = id;
         this.callbackURL = callbackURL;
         this.topic = topic;
         this.endpointId = endpointId;
      }

      /**
       * Creates a subscription builder with a topic id and endpoint id.
       * @param id The id.
       * @param callbackURL The callback URL.
       * @param topicId The topic id.
       * @param endpointId The endpoint id.
       */
      public Builder(final long id, final String callbackURL, final long topicId, final long endpointId) {
         this.id = id;
         this.callbackURL = callbackURL;
         this.topicId = topicId;
         this.endpointId = endpointId;
      }

      /**
       * Creates a subscription builder with a topic id and endpoint id.
       * @param id The id.
       * @param callbackURL The callback URL.
       * @param topicId The topic id.
       * @param endpoint The endpoint.
       */
      public Builder(final long id, final String callbackURL, final long topicId, final Endpoint endpoint) {
         this.id = id;
         this.callbackURL = callbackURL;
         this.topicId = topicId;
         this.endpoint = endpoint;
         this.endpointId = endpoint.getId();
      }

      /**
       * Creates a subscription builder with a known topic and endpoint id.
       * @param id The id.
       * @param callbackURL The callback URL.
       * @param topic The topic.
       * @param endpoint The endpoint.
       */
      public Builder(final long id, final String callbackURL, final Topic topic, final Endpoint endpoint) {
         this.id = id;
         this.callbackURL = callbackURL;
         this.topic = topic;
         this.endpoint = endpoint;
         this.endpointId = endpoint.getId();
      }

      /**
       * Creates a builder from an existing subscription and endpoint.
       * @param subscription The subscription.
       */
      public Builder(final Subscription subscription) {
         this.id = subscription.id;
         this.callbackURL = subscription.callbackURL;
         this.topic = subscription.topic;
         this.secret = subscription.secret;
         this.leaseSeconds = subscription.leaseSeconds;
         this.status = subscription.status;
         this.expireTime = subscription.expireTime;
         this.endpoint = subscription.endpoint;
         this.endpointId = subscription.getEndpointId();
      }

      /**
       * Creates a builder from an existing subscription and endpoint.
       * @param subscription The subscription.
       * @param endpoint The endpoint.
       */
      public Builder(final Subscription subscription, final Endpoint endpoint) {
         this.id = subscription.id;
         this.callbackURL = subscription.callbackURL;
         this.topic = subscription.topic;
         this.secret = subscription.secret;
         this.leaseSeconds = subscription.leaseSeconds;
         this.status = subscription.status;
         this.expireTime = subscription.expireTime;
         this.endpoint = endpoint;
         this.endpointId = endpoint.getId();
      }

      /**
       * Creates an immutable <code>Subscription</code>
       * @return The subscription.
       */
      public Subscription create() {
         return new Subscription(id, callbackURL, topic, endpointId, secret, leaseSeconds, status, expireTime, endpoint);
      }

      /**
       * Sets the HMAC secret.
       * @param secret The secret.
       * @return A self-reference.
       */
      public Builder setSecret(final String secret) {
         this.secret = secret;
         return this;
      }

      /**
       * Sets the lease seconds.
       * @param leaseSeconds The lease in seconds.
       * @return A self-reference.
       */
      public Builder setLeaseSeconds(final int leaseSeconds) {
         this.leaseSeconds = leaseSeconds;
         return this;
      }

      /**
       * Sets the status.
       * @param status The status.
       * @return A self-reference.
       */
      public Builder setStatus(final Status status) {
         this.status = status;
         return this;
      }

      /**
       * Sets the expire time.
       * @param expireTime The expire time.
       * @return A self-reference.
       */
      public Builder setExpireTime(final Date expireTime) {
         this.expireTime = expireTime;
         return this;
      }

      /**
       * Sets the topic id.
       * @param topicId The topic id.
       * @return A self-reference.
       */
      public Builder setTopicId(final long topicId) {
         this.topicId = topicId;
         return this;
      }

      /**
       * Gets the topic id.
       * @return The topic id.
       */
      public long getTopicId() {
         return topicId;
      }

      /**
       * Sets the topic.
       * @param topic The topic.
       * @return A self-reference.
       */
      public Builder setTopic(final Topic topic) {
         this.topic = topic;
         return this;
      }

      /**
       * Gets the topic.
       * @return The topic.
       */
      public Topic getTopic() {
         return topic;
      }

      /**
       * Determines if the topic is set.
       * @return Is the topic set?
       */
      public boolean topicIsSet() {
         return topic != null;
      }

      protected final long id;
      protected final long endpointId;
      protected final String callbackURL;

      protected String secret;
      protected int leaseSeconds;
      protected Status status;
      protected Date expireTime;
      protected Endpoint endpoint;
      protected Topic topic;
      protected long topicId;
   }


   /**
    * Defines allowed subscription status.
    */
   public enum Status {

      /**
       * Subscription is pending.
       */
      PENDING(0),

      /**
       * Subscription is active.
       */
      ACTIVE(1),

      /**
       * Subscription is expired.
       */
      EXPIRED(2),

      /**
       * Subscription was explicitly removed.
       */
      REMOVED(3),

      /**
       * Subscription unsubscribe is pending.
       */
      PENDING_UNSUBSCRIBE(4),

      /**
       * Subscription state is invalid or unknown.
       */
      INVALID(127);

      Status(final int value) {
         this.value = value;
      }

      /**
       * Gets a status from the integer value.
       * @param value The value.
       * @return The status.
       */
      public static final Status fromValue(int value) {
         switch(value) {
            case 0:
               return PENDING;
            case 1:
               return ACTIVE;
            case 2:
               return EXPIRED;
            case 3:
               return REMOVED;
            case 4:
               return PENDING_UNSUBSCRIBE;
            default:
               return INVALID;
         }
      }

      /**
       * Gets an integer value for this status.
       * @return The assigned integer value.
       */
      public int getValue() {
         return value;
      }

      private final int value;

   }

   public Subscription(final long id, final String callbackURL, final Topic topic, final long endpointId,
                       final String secret, final int leaseSeconds, final Status status, final Date expireTime, final Endpoint endpoint) {

      if(topic == null) {
         throw new UnsupportedOperationException("Topic must not be null");
      }

      if(Strings.isNullOrEmpty(callbackURL)) {
         throw new UnsupportedOperationException("Callback URL must not be null or empty");
      }

      this.id = id;
      this.callbackURL = callbackURL.trim();
      this.topic = topic;
      this.endpointId = endpointId;

      int hi = this.callbackURL.indexOf("://");
      if(hi < 0) {
         hi = 0;
      } else {
         hi += 3;
      }

      int pi = this.callbackURL.indexOf('/', hi);

      if(pi < 0) {
         this.callbackHost = callbackURL.substring(hi);
         this.callbackPath = "/";
      } else {
         this.callbackHost = callbackURL.substring(hi, pi);
         this.callbackPath = callbackURL.substring(pi);
      }

      this.secret = secret;
      this.leaseSeconds = leaseSeconds;
      this.status = status;
      this.expireTime = expireTime;
      this.endpoint = endpoint;
   }

   public Subscription(final long id, final String callbackURL, final Topic topic, final long endpointId) {

      if(topic == null) {
         throw new UnsupportedOperationException("Topic must not be null");
      }

      if(Strings.isNullOrEmpty(callbackURL)) {
         throw new UnsupportedOperationException("Callback URL must not be null or empty");
      }

      this.id = id;
      this.callbackURL = callbackURL.trim();
      this.topic = topic;
      this.endpointId = endpointId;

      int hi = this.callbackURL.indexOf("://");
      if(hi < 0) {
         hi = 0;
      } else {
         hi += 3;
      }

      int pi = this.callbackURL.indexOf('/', hi);

      if(pi < 0) {
         this.callbackHost = callbackURL.substring(hi);
         this.callbackPath = "/";
      } else {
         this.callbackHost = callbackURL.substring(hi, pi);
         this.callbackPath = callbackURL.substring(pi);
      }
   }

   /**
    * Creates a subscription with a unique id.
    * @param id The id.
    * @param callbackURL The callback URL.
    * @param topic The topic.
    */
   public Subscription(final long id, final String callbackURL, final Topic topic) {
      this(id, callbackURL, topic, 0L);
   }

   /**
    * Creates a subscription.
    * @param callbackURL The callback URL.
    * @param topic The topic.
    */
   public Subscription(final String callbackURL, final Topic topic) {
      this(0L, callbackURL, topic, 0L);
   }

   /**
    * Copies from another subscription.
    * @param other The other subscription.
    */
   public Subscription(final Subscription other) {
      this.id = other.id;
      this.callbackURL = other.callbackURL;
      this.callbackHost = other.callbackHost;
      this.callbackPath = other.callbackPath;
      this.topic = other.topic;
      this.endpointId = other.endpointId;
      this.secret = other.secret;
      this.status = other.status;
      this.leaseSeconds = other.leaseSeconds;
      this.endpoint = other.endpoint;
   }

   @Override
   /**
    * Subscriptions are equal when their topic and callback URLs are equal.
    */
   public boolean equals(Object other) {
      if(other instanceof Subscription) {
         Subscription otherSubscription = (Subscription)other;
         return otherSubscription.callbackURL.equals(callbackURL) && otherSubscription.topic.equals(topic);
      } else {
         return false;
      }
   }

   @Override
   public int hashCode() {
      int h = 17;
      h = 31 * h + callbackURL.hashCode();
      h = 31 * h + topic.hashCode();
      return h;
   }

   /**
    * Gets the topic.
    * @return The topic.
    */
   public Topic getTopic() {
      return topic;
   }

   /**
    * Gets the secret used to send authenticated notifications.
    * @return The secret or <code>null</code> if none specified.
    */
   public String getSecret() {
      return secret;
   }

   /**
    * Gets the subscription lease in seconds. If <code>0</code> lease is
    * indefinite.
    * @return The subscription lease in seconds.
    */
   public int getLeaseSeconds() {
      return leaseSeconds;
   }

   /**
    * Gets the unique id assigned to this subscription.
    * @return The id.
    */
   public long getId() {
      return id;
   }

   /**
    * Determines if the subscription is active.
    * @return Is the subscription active?
    */
   public boolean isActive() {
      return status != null && status == Status.ACTIVE;
   }

   /**
    * Determines if the subscription is disabled/removed.
    * @return Is the subscription removed?
    */
   public boolean isRemoved() {
      return status != null && status == Status.REMOVED;
   }

   /**
    * Determines if the subscription is expired.
    * @return Is the subscription expired?
    */
   public boolean isExpired() {
      return status != null && status == Status.EXPIRED;
   }

   /**
    * Gets the subscription status.
    * @return The status.
    */
   public Status getStatus() {
      return status;
   }

   /**
    * Gets the id of the endpoint.
    * @return The endpoint id.
    */
   public long getEndpointId() {
      return endpointId;
   }

   /**
    * Gets the callback URL.
    * @return The callback URL.
    */
   public String getCallbackURL() {
      return callbackURL;
   }

   /**
    * Gets the host of the callback URL.
    * @return The host.
    */
   public String getCallbackHost() {
      return callbackHost;
   }

   /**
    * Gets the callback URL's path.
    * @return The path.
    */
   public String getCallbackPath() {
      return callbackPath;
   }

   /**
    * Returns the current expire time.
    * @return The expire time.
    */
   public Date getExpireTime() {
      return expireTime;
   }

   protected final long id;
   protected final long endpointId;
   protected final Topic topic;
   protected final String callbackURL;
   protected final String callbackHost;
   protected final String callbackPath;

   protected String secret;
   protected int leaseSeconds;
   protected Status status;
   protected Date expireTime;
   protected Endpoint endpoint;

}
