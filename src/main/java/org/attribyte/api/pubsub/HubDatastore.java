/*
 * Copyright 2010 Attribyte, LLC 
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

import com.codahale.metrics.MetricSet;
import com.codahale.metrics.health.HealthCheck;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.http.Request;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Defines methods required for storing and retrieving data for hub operations.
 */
public interface HubDatastore {

   /**
    * Indicates no more ids.
    */
   public static final long LAST_ID = Long.MAX_VALUE;

   /**
    * Reports significant internal events.
    */
   public interface EventHandler {

      /**
       * Reports the creation of a new topic.
       * @param topic The new topic.
       * @throws DatastoreException if topic was not created.
       */
      public void newTopic(Topic topic) throws DatastoreException;

      /**
       * Reports the creation of a new (verified) subscription.
       * @param subscription The new subscription.
       * @throws DatastoreException if subscription was not created.
       */
      public void newSubscription(Subscription subscription) throws DatastoreException;

      /**
       * Reports any abnormal exceptions.
       * @param t The (throwable) exception.
       */
      public void exception(Throwable t);

      /**
       * Sets a handler to which events are reported after they
       * are handled by this handler.
       * @param next The next handler.
       */
      public void setNext(EventHandler next);
   }

   /**
    * Initializes the data store.
    * <p>
    * Must be called <em>once</em> before any operations.
    * </p>
    * @param prefix The prefix for all applicable properties (e.g. 'datastore.').
    * @param props The properties.
    * @param eventHandler An event handler. May be <code>null</code>.
    * @param logger A logger. Must not be <code>null</code>.
    * @throws org.attribyte.api.InitializationException on initialization problem.
    */
   public void init(String prefix, Properties props, EventHandler eventHandler, Logger logger) throws InitializationException;

   /**
    * Gets a topic by id.
    * @param topicId The topic id.
    * @return The topic or <code>null</code> if not found.
    * @throws DatastoreException on datastore error.
    */
   public Topic getTopic(long topicId) throws DatastoreException;

   /**
    * Gets a topic, optionally creating a new topic if the topic URL is new.
    * @param topicURL The topic URL.
    * @param create Should a new topic be created if the topic URL is new?
    * @return The topic, or <code>null</code> if none found or created.
    * @throws DatastoreException on datastore error.
    */
   public Topic getTopic(String topicURL, boolean create) throws DatastoreException;

   /**
    * Checks to see if subscribing to a topic requires auth.
    * @param topicId The topic id.
    * @return Does the topic require auth for subscription?
    * @throws DatastoreException on datastore error.
    */
   public boolean requiresSubscriptionAuth(long topicId) throws DatastoreException;

   /**
    * Gets subscription auth for a topic.
    * @param topicId The topic id.
    * @param authId The (client) auth id.
    * @param authScheme The auth scheme.
    * @return The auth hash or <tt>null</tt> if none.
    * @throws DatastoreException
    */
   public String getSubscriptionAuth(long topicId, String authId, String authScheme) throws DatastoreException;


   /**
    * Gets a subscription by id.
    * @param subscriptionId The id.
    * @return The subscription or <code>null</code> if not found.
    * @throws DatastoreException on datastore error.
    */
   public Subscription getSubscription(long subscriptionId) throws DatastoreException;

   /**
    * Gets a list of all subscriptions.
    * @param start The start index.
    * @param limit The maximum number returned.
    * @return The list of subscriptions.
    * @throws DatastoreException on datastore error.
    */
   public List<Subscription> getSubscriptions(int start, int limit) throws DatastoreException;

   /**
    * Updates a subscription for a topic and callback.
    * <p>
    * If a subscription does not exist, one will be created.
    * Lease will only be extended if the subscription is active.
    * </p>
    * @param subscription The subscription to update.
    * @param extendLease Should the current lease be extended?
    * @return The updated subscription.
    * @throws DatastoreException on datastore error.
    */
   public Subscription updateSubscription(Subscription subscription, boolean extendLease) throws DatastoreException;

   /**
    * Changes the status of a subscription.
    * @param id The subscription id.
    * @param newStatus The new status for the subscription.
    * @param newLeaseSeconds The new lease. Applied only if > 0.
    * @throws DatastoreException if subscription does not exist, or other datastore error.
    */
   public void changeSubscriptionStatus(long id, Subscription.Status newStatus, int newLeaseSeconds) throws DatastoreException;

   /**
    * Sets status to "expired" for all active subscriptions past their expiration time.
    * @param maxExpired The maximum number expired.
    * @return The number of subscriptions expired.
    * @throws DatastoreException on datastore error.
    */
   public int expireSubscriptions(int maxExpired) throws DatastoreException;

   /**
    * Gets the ids of active subscriptions for a topic.
    * <p>
    * This method allows efficient, but not the most straightforward, paging
    * by returning subscription ids in the order they were created (ascending id).
    * The method returns the id that will start the next page.
    * If this id is <code>LAST_ID</code>, there are no more pages. Otherwise, the returned id is passed
    * into the next call as <code>startId</code> to get the next page.
    * </p>
    * @param topic The topic.
    * @param subscriptions A collection to fill with subscriptions.
    * @param startId The starting subscription id. The first returned subscription will be >= this id.
    * @param maxReturned The maximum number of subscriptions returned.
    * @return The <code>startId</code> for the next page or 0 if no more pages.
    * @throws DatastoreException on datastore error.
    */
   public long getActiveSubscriptions(Topic topic, Collection<Subscription> subscriptions, long startId, int maxReturned) throws DatastoreException;

   /**
    * Determine if a topic has any active subscriptions.
    * @param topicId The topic id.
    * @return Does the topic have any active subscriptions?
    * @throws DatastoreException on datastore error.
    */
   public boolean hasActiveSubscriptions(long topicId) throws DatastoreException;

   /**
    * Determine if a callback URL has any associated active subscriptions.
    * @param callbackURL The callback URL.
    * @return Does the callback have any active subscriptions?
    * @throws DatastoreException on datastore err.r
    */
   public boolean hasActiveSubscriptions(String callbackURL) throws DatastoreException;

   /**
    * Gets a list of all subscriptions mapped to a callback path.
    * @param callbackPath The path.
    * @return The list of subscriptions.
    * @throws DatastoreException on datastore error.
    */
   public List<Subscription> getSubscriptionsForPath(String callbackPath) throws DatastoreException;

   /**
    * Resolve an auth scheme from the scheme name.
    * @param scheme The scheme name.
    * @return The auth scheme or <code>null</code> if none.
    * @throws DatastoreException on datastore error.
    */
   public AuthScheme resolveAuthScheme(String scheme) throws DatastoreException;

   /**
    * Adds endpoint-specific auth.
    * @param endpoint The endpoint.
    * @param request The request.
    * @return The request with auth added.
    * @throws DatastoreException on datastore error.
    */
   public Request addAuth(Endpoint endpoint, Request request) throws DatastoreException;

   /**
    * Shutdown the datatastore.
    */
   public void shutdown();

   /**
    * Gets a metric set for the datastore.
    * @return The metric set.
    */
   public MetricSet getMetrics();

   /**
    * Gets a collection of health checks for the datastore.
    * @return The collection of health checks.
    */
   public Map<String, HealthCheck> getHealthChecks();

   /**
    * Gets a subscription for a topic and callback.
    * @param topicURL The topic URL
    * @param callbackURL The callback URL
    * @return The subscription or <code>null</code> if none exists.
    * @throws DatastoreException on datastore error.
    */
   public Subscription getSubscription(String topicURL, String callbackURL) throws DatastoreException;

   /**
    * Gets a subscriber.
    * @param endpointURL The subscriber's endpoint URL.
    * @param scheme The auth scheme. May be <code>null</code>.
    * @param authId The auth id. Should be <code>null</code> when scheme is unspecified.
    * @param create If <code>true</code> and a matching subscription is not found, one will be created and returned.
    * @return The subscriber, or <code>null</code> if not found.
    * @throws DatastoreException on datastore error.
    */
   public Subscriber getSubscriber(String endpointURL, AuthScheme scheme, String authId, boolean create) throws DatastoreException;

   /**
    * Gets a subscriber.
    * @param id The subscriber id.
    * @return The subscriber or <code>null</code> if not found.
    * @throws DatastoreException on datastore error.
    */
   public Subscriber getSubscriber(long id) throws DatastoreException;

   /**
    * Creates a subscriber if none exists for the combination of endpoint URL and authentication scheme.
    * @param endpointURL The subscriber's endpoint URL.
    * @param scheme The auth scheme. May be <code>null</code>.
    * @param authId The auth id. Should be <code>null</code> when scheme is unspecified.
    * @return The subscriber.
    * @throws DatastoreException on datastore error.
    */
   public Subscriber createSubscriber(String endpointURL, AuthScheme scheme, String authId) throws DatastoreException;
}