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

package org.attribyte.api.pubsub.impl;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.http.RequestBuilder;
import org.attribyte.api.http.impl.BasicAuthScheme;
import org.attribyte.api.pubsub.*;
import org.attribyte.util.SQLUtil;

import java.sql.*;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Defines methods required for storing and retrieving hub-specific data.
 */
public abstract class RDBHubDatastore implements HubDatastore {

   /**
    * Gets a database connection.
    * @return The connection.
    * @throws SQLException if connection is unavailable.
    */
   public abstract Connection getConnection() throws SQLException;

   private static final String getTopicIdSQL = "SELECT topicURL, createTime FROM topic WHERE id=?";

   @Override
   public final Topic getTopic(final long topicId) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getTopicIdSQL);
         stmt.setLong(1, topicId);
         rs = stmt.executeQuery();
         return rs.next() ? new Topic(rs.getString(1), topicId, new Date(rs.getTimestamp(2).getTime())) : null;
      } catch(SQLException se) {
         logger.error("Problem getting topic", se);
         throw new DatastoreException("Problem getting topic", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String getTopicSQL = "SELECT id, createTime FROM topic WHERE topicURL=?";
   private static final String createTopicSQL = "INSERT IGNORE INTO topic (topicURL, topicHash) VALUES (?, MD5(?))";

   @Override
   public final Topic getTopic(final String topicURL, final boolean create) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Topic newTopic = null;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getTopicSQL);
         stmt.setString(1, topicURL);
         rs = stmt.executeQuery();
         if(rs.next()) {
            return new Topic(topicURL, rs.getLong(1), new Date(rs.getTimestamp(2).getTime()));
         } else if(create) {
            SQLUtil.closeQuietly(stmt, rs);
            rs = null;
            stmt = null;

            stmt = conn.prepareStatement(createTopicSQL, Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, topicURL);
            stmt.setString(2, topicURL);
            if(stmt.executeUpdate() == 0) { //Topic created elsewhere after our check
               SQLUtil.closeQuietly(conn, stmt);
               conn = null;
               stmt = null;
               return getTopic(topicURL, false);
            } else {
               rs = stmt.getGeneratedKeys();
               if(rs.next()) {
                  newTopic = new Topic(topicURL, rs.getLong(1), new Date());
                  return newTopic;
               } else {
                  throw new DatastoreException("Problem creating topic: Expecting 'id' to be generated.");
               }
            }
         } else {
            return null;
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting topic", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
         if(newTopic != null && eventHandler != null) {
            eventHandler.newTopic(newTopic);
         }
      }
   }

   private static final String getTopicsSQL = "SELECT topicURL, id, createTime FROM topic ORDER BY id DESC LIMIT ?,?";

   @Override
   public List<Topic> getTopics(int start, int limit) throws DatastoreException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Topic> topics = Lists.newArrayListWithExpectedSize(limit < 512 ? limit : 512);
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getTopicsSQL);
         stmt.setInt(1, start);
         stmt.setInt(2, limit);
         rs = stmt.executeQuery();
         while(rs.next()) {
            topics.add(new Topic(rs.getString(1), rs.getLong(2), new Date(rs.getTimestamp(3).getTime())));
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem selecting topics", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      return topics;
   }

   private static final String getActiveTopicIdsSQL =
           "SELECT DISTINCT topicId FROM subscription WHERE status=" + Subscription.Status.ACTIVE.getValue() +
                   " ORDER BY topicId DESC";

   @Override
   public List<Topic> getActiveTopics(int start, int limit) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Long> topicIds = Lists.newArrayListWithExpectedSize(512);
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getActiveTopicIdsSQL);
         rs = stmt.executeQuery();
         while(rs.next()) {
            topicIds.add(rs.getLong(1));
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem selecting topics", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      if(start >= topicIds.size()) {
         return Collections.emptyList();
      }

      List<Topic> topics = Lists.newArrayListWithExpectedSize(limit < 512 ? limit : 512);

      int toIndex = start + limit;
      if(toIndex > topicIds.size()) {
         toIndex = topicIds.size();
      }

      topicIds = topicIds.subList(start, toIndex);
      for(long topicId : topicIds) {
         Topic topic = getTopic(topicId);
         if(topic != null) {
            topics.add(topic);
         }
      }

      return topics;
   }


   private static final String hasActiveSubscriptionSQL = "SELECT 1 FROM subscription WHERE topicId=?" +
           " AND status=" + Subscription.Status.ACTIVE.getValue() + " LIMIT 1";

   @Override
   public final boolean hasActiveSubscriptions(final long topicId) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(hasActiveSubscriptionSQL);
         stmt.setLong(1, topicId);
         rs = stmt.executeQuery();
         return rs.next();
      } catch(SQLException se) {
         throw new DatastoreException("Problem checking active subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String countActiveSubscriptionSQL = "SELECT COUNT(id) FROM subscription WHERE topicId=?" +
           " AND status=" + Subscription.Status.ACTIVE.getValue();

   public int countActiveSubscriptions(long topicId) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(countActiveSubscriptionSQL);
         stmt.setLong(1, topicId);
         rs = stmt.executeQuery();
         return rs.next() ? rs.getInt(1) : 0;
      } catch(SQLException se) {
         throw new DatastoreException("Problem counting active subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String hasActiveCallbackSubscriptionSQL = "SELECT 1 FROM subscription WHERE callbackURL=?" +
           " AND status=" + Subscription.Status.ACTIVE.getValue() + " LIMIT 1";

   @Override
   public boolean hasActiveSubscriptions(final String callbackURL) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(hasActiveCallbackSubscriptionSQL);
         stmt.setString(1, callbackURL);
         rs = stmt.executeQuery();
         return rs.next();
      } catch(SQLException se) {
         throw new DatastoreException("Problem checking active subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   protected static final String getSubscriptionSQL =
           "SELECT id, endpointId, topicId, callbackURL, status, leaseSeconds, hmacSecret, expireTime FROM subscription WHERE ";

   private static final String getIdSubscriptionSQL = getSubscriptionSQL + "id=?";

   @Override
   public final Subscription getSubscription(final long id) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      final Subscription.Builder builder;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getIdSubscriptionSQL);
         stmt.setLong(1, id);
         rs = stmt.executeQuery();
         if(rs.next()) {
            builder = getSubscription(rs);
         } else {
            return null;
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscription", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      Topic topic = getTopic(builder.getTopicId());
      if(topic == null) {
         throw new DatastoreException("No topic found for id = " + builder.getTopicId());
      } else {
         return builder.setTopic(topic).create();
      }
   }

   private static Joiner inJoiner = Joiner.on(',');

   private static void inStatus(final Collection<Subscription.Status> statusList, final StringBuilder sql) {
      if(statusList == null || statusList.isEmpty()) {
         sql.append("1");
      } else if(statusList.size() == 1) {
         sql.append("status=").append(statusList.iterator().next().getValue());
      } else {
         List<String> statusCodes = Lists.newArrayListWithCapacity(statusList.size());
         for(Subscription.Status status : statusList) statusCodes.add(Integer.toString(status.getValue()));
         sql.append("status IN (");
         sql.append(inJoiner.join(statusCodes));
         sql.append(")");
      }
   }

   @Override
   public List<Subscription> getSubscriptions(final Collection<Subscription.Status> status,
                                              final int start, final int limit) throws DatastoreException {

      StringBuilder sql = new StringBuilder(getSubscriptionSQL);
      inStatus(status, sql);
      sql.append(" ORDER BY id ASC LIMIT ?,?");

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Subscription.Builder> subscriptionBuilders = Lists.newArrayListWithExpectedSize(limit < 1024 ? limit : 1024);

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setInt(1, start);
         stmt.setInt(2, limit);
         rs = stmt.executeQuery();
         while(rs.next()) {
            subscriptionBuilders.add(getSubscription(rs));
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Subscription> subscriptions = Lists.newArrayListWithCapacity(subscriptionBuilders.size());

      for(Subscription.Builder builder : subscriptionBuilders) {
         Topic topic = getTopic(builder.getTopicId());
         if(topic == null) {
            throw new DatastoreException("No topic found for id = " + builder.getTopicId());
         } else {
            builder.setTopic(topic);
            subscriptions.add(builder.create());
         }
      }

      return subscriptions;
   }

   @Override
   public final List<Subscription> getHostSubscriptions(final String callbackHost,
                                                        final Collection<Subscription.Status> status,
                                                        final int start, final int limit) throws DatastoreException {

      StringBuilder sql = new StringBuilder(getSubscriptionSQL);
      sql.append(" callbackHost=? AND ");
      inStatus(status, sql);
      sql.append(" ORDER BY id ASC LIMIT ?,?");

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Subscription.Builder> subscriptionBuilders = Lists.newArrayListWithExpectedSize(limit < 1024 ? limit : 1024);

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setString(1, callbackHost);
         stmt.setInt(2, start);
         stmt.setInt(3, limit);
         rs = stmt.executeQuery();
         while(rs.next()) {
            subscriptionBuilders.add(getSubscription(rs));
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Subscription> subscriptions = Lists.newArrayListWithCapacity(subscriptionBuilders.size());

      for(Subscription.Builder builder : subscriptionBuilders) {
         Topic topic = getTopic(builder.getTopicId());
         if(topic == null) {
            throw new DatastoreException("No topic found for id = " + builder.getTopicId());
         } else {
            builder.setTopic(topic);
            subscriptions.add(builder.create());
         }
      }

      return subscriptions;
   }

   @Override
   public List<Subscription> getTopicSubscriptions(Topic topic,
                                                   final Collection<Subscription.Status> status,
                                                   int start, int limit) throws DatastoreException {

      StringBuilder sql = new StringBuilder(getSubscriptionSQL);
      sql.append(" topicId=? AND ");
      inStatus(status, sql);
      sql.append(" ORDER BY id ASC LIMIT ?,?");

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Subscription.Builder> subscriptionBuilders = Lists.newArrayListWithExpectedSize(limit < 1024 ? limit : 1024);

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(sql.toString());
         stmt.setLong(1, topic.getId());
         stmt.setInt(2, start);
         stmt.setInt(3, limit);
         rs = stmt.executeQuery();
         while(rs.next()) {
            subscriptionBuilders.add(getSubscription(rs));
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Subscription> subscriptions = Lists.newArrayListWithCapacity(subscriptionBuilders.size());

      for(Subscription.Builder builder : subscriptionBuilders) {
         builder.setTopic(topic);
         subscriptions.add(builder.create());
      }

      return subscriptions;
   }

   private static final String updateSubscriptionSQL = "UPDATE subscription SET endpointId=?, status=?, leaseSeconds=?, hmacSecret=? WHERE id=?";
   private static final String updateSubscriptionExtendLeaseSQL =
           "UPDATE subscription SET endpointId=?, status=?, leaseSeconds=?, hmacSecret=?, expireTime=NOW() + INTERVAL ? SECOND WHERE id=?";
   private static final String createSubscriptionSQL =
           "INSERT INTO subscription (endpointId, topicId, callbackURL, callbackHash, callbackHost, callbackPath, status, createTime, leaseSeconds, hmacSecret, expireTime)" +
                   "VALUES (?,?,?,MD5(?),?,?,?,NOW(),?,?,NOW()+INTERVAL ? SECOND) ON DUPLICATE KEY UPDATE endpointId=?, status=?, leaseSeconds=?, hmacSecret=?, expireTime=NOW()+INTERVAL ? SECOND";

   @Override
   public final Subscription updateSubscription(final Subscription subscription, boolean extendLease) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      Subscription currSubscription = getSubscription(subscription.getId());
      Subscription newSubscription = null;

      try {

         if(currSubscription == null) {

            Topic topic = subscription.getTopic();
            if(topic.getId() < 1) {
               topic = getTopic(topic.getURL(), true); //Create a new topic...
            }

            conn = getConnection();
            stmt = conn.prepareStatement(createSubscriptionSQL, Statement.RETURN_GENERATED_KEYS);
            stmt.setLong(1, subscription.getEndpointId());
            stmt.setLong(2, topic.getId());
            stmt.setString(3, subscription.getCallbackURL());
            stmt.setString(4, subscription.getCallbackURL());
            stmt.setString(5, subscription.getCallbackHost());
            stmt.setString(6, subscription.getCallbackPath());
            stmt.setInt(7, subscription.getStatus().getValue());
            stmt.setInt(8, subscription.getLeaseSeconds() < 0 ? 0 : subscription.getLeaseSeconds());
            stmt.setString(9, subscription.getSecret());
            stmt.setInt(10, subscription.getLeaseSeconds());
            stmt.setLong(11, subscription.getEndpointId());
            stmt.setInt(12, subscription.getStatus().getValue());
            stmt.setInt(13, subscription.getLeaseSeconds() < 0 ? 0 : subscription.getLeaseSeconds());
            stmt.setString(14, subscription.getSecret());
            stmt.setInt(15, subscription.getLeaseSeconds());
            stmt.executeUpdate();
            rs = stmt.getGeneratedKeys();
            if(rs.next()) {
               long newId = rs.getLong(1);
               SQLUtil.closeQuietly(conn, stmt, rs);
               conn = null;
               stmt = null;
               rs = null;
               newSubscription = getSubscription(newId);
               return newSubscription;
            } else {
               throw new DatastoreException("Problem creating Subscription: Expecting id to be generated");
            }
         } else {
            Subscription.Status newStatus = subscription.getStatus();

            if(newStatus != Subscription.Status.ACTIVE) {
               extendLease = false; //Never extend the lease for inactive subscriptions.
            }

            conn = getConnection();
            if(!extendLease) {
               stmt = conn.prepareStatement(updateSubscriptionSQL);
            } else {
               stmt = conn.prepareStatement(updateSubscriptionExtendLeaseSQL);
            }

            stmt.setLong(1, subscription.getEndpointId());
            stmt.setInt(2, newStatus.getValue());
            stmt.setInt(3, subscription.getLeaseSeconds() < 0 ? 0 : subscription.getLeaseSeconds());
            stmt.setString(4, subscription.getSecret());
            if(!extendLease) {
               stmt.setLong(5, subscription.getId());
            } else {
               stmt.setInt(5, subscription.getLeaseSeconds());
               stmt.setLong(6, subscription.getId());
            }
         }

         stmt.executeUpdate();
         SQLUtil.closeQuietly(conn, stmt); //Avoid using two connections concurrently...
         conn = null;
         stmt = null;
         rs = null;

         return getSubscription(subscription.getId());

      } catch(SQLException se) {
         throw new DatastoreException("Problem updating subscription", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
         if(newSubscription != null && eventHandler != null) {
            eventHandler.newSubscription(newSubscription);
         }
      }
   }

   private static final String changeSubscriptionStatusSQL = "UPDATE subscription SET status=? WHERE id=?";
   private static final String changeSubscriptionStatusLeaseSQL = "UPDATE subscription SET status=?, leaseSeconds=?, expireTime = NOW() + INTERVAL ? SECOND WHERE id=?";

   @Override
   public final void changeSubscriptionStatus(final long id, final Subscription.Status newStatus, final int newLeaseSeconds) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;

      try {
         conn = getConnection();
         if(newLeaseSeconds > 0) {
            stmt = conn.prepareStatement(changeSubscriptionStatusLeaseSQL);
            stmt.setInt(1, newStatus.getValue());
            stmt.setInt(2, newLeaseSeconds);
            stmt.setInt(3, newLeaseSeconds);
            stmt.setLong(4, id);
         } else {
            stmt = conn.prepareStatement(changeSubscriptionStatusSQL);
            stmt.setInt(1, newStatus.getValue());
            stmt.setLong(2, id);
         }

         if(stmt.executeUpdate() == 0) {
            throw new DatastoreException("The subscription with id=" + id + " does not exist");
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem updating subscription", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   //Note that this works because "createTime" is actually updated anytime
   //a subscription is modified. Probably it should be renamed (someday)...

   private static final String getSubscriptionStateSQL = "SELECT MAX(createTime) FROM subscription WHERE topicId=?";

   @Override
   public org.attribyte.api.pubsub.SubscriptionState getSubscriptionState(Topic topic) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getSubscriptionStateSQL);
         stmt.setLong(1, topic.getId());
         rs = stmt.executeQuery();
         if(rs.next()) {
            return new SubscriptionState(rs.getTimestamp(1).getTime());
         } else {
            return new SubscriptionState(0L);
         }

      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscription state", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String getActiveSubscriptionsSQL = getSubscriptionSQL +
           " topicId=? AND id >= ? AND status=" + Subscription.Status.ACTIVE.getValue() + " ORDER BY id ASC LIMIT ?";

   @Override
   public final long getActiveSubscriptions(Topic topic, final Collection<Subscription> subscriptions, final long startId, final int maxReturned) throws DatastoreException {

      if(startId == HubDatastore.LAST_ID) {
         throw new DatastoreException("The 'startId' is invalid"); //Avoid possible infinite loop when paging.
      }

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      String topicURL = topic.getURL();

      if(topic.getId() < 1) {
         topic = getTopic(topicURL, false);
      }

      if(topic == null) {
         throw new DatastoreException("No topic found for '" + topicURL + "'");
      }

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getActiveSubscriptionsSQL);
         stmt.setLong(1, topic.getId());
         stmt.setLong(2, startId);
         stmt.setInt(3, maxReturned + 1);
         rs = stmt.executeQuery();
         int count = 0;
         while(rs.next()) {
            long id = rs.getLong(1);
            if(count < maxReturned) {
               subscriptions.add(getSubscription(rs).setTopic(topic).create());
               count++;
            } else {
               return id;
            }
         }

         return HubDatastore.LAST_ID;

      } catch(SQLException se) {
         throw new DatastoreException("Problem getting active subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String getActivePathSubscriptionsSQL = getSubscriptionSQL + "callbackPath=? AND status=" + Subscription.Status.ACTIVE.getValue();

   @Override
   public final List<Subscription> getSubscriptionsForPath(final String callbackPath) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<Subscription.Builder> subscriptionBuilders = Lists.newArrayListWithExpectedSize(256);

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getActivePathSubscriptionsSQL);
         stmt.setString(1, callbackPath);
         rs = stmt.executeQuery();
         while(rs.next()) {
            subscriptionBuilders.add(getSubscription(rs));
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting path subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      List<Subscription> subscriptions = Lists.newArrayListWithCapacity(subscriptionBuilders.size());

      for(Subscription.Builder builder : subscriptionBuilders) {
         Topic topic = getTopic(builder.getTopicId());
         if(topic == null) {
            throw new DatastoreException("No topic found for id = " + builder.getTopicId());
         } else {
            subscriptions.add(builder.setTopic(topic).create());
         }
      }

      return subscriptions;
   }

   private static final String expireSubscriptionsSQL = "UPDATE subscription SET status=" + Subscription.Status.EXPIRED.getValue() +
           " WHERE status=" + Subscription.Status.ACTIVE.getValue() + " AND expireTime < NOW() LIMIT ?";

   @Override
   public int expireSubscriptions(final int maxExpired) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(expireSubscriptionsSQL);
         stmt.setInt(1, maxExpired);
         return stmt.executeUpdate();
      } catch(SQLException se) {
         throw new DatastoreException("Problem expiring subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   private static final String expireSubscriptionIdSQL = "UPDATE subscription SET status=" + Subscription.Status.EXPIRED.getValue() +
           ", expireTime=NOW() WHERE id=?";

   @Override
   public void expireSubscription(long id) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(expireSubscriptionIdSQL);
         stmt.setLong(1, id);
         stmt.executeUpdate();
      } catch(SQLException se) {
         throw new DatastoreException("Problem expiring subscription", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt);
      }
   }

   /**
    * Gets a subscription builder from a result set.
    * <p>
    * The topic must be resolved from the topic id.
    * </p>
    * @param rs The result set.
    * @return The subscription builder.
    * @throws DatastoreException on database error.
    */
   protected final Subscription.Builder getSubscription(final ResultSet rs) throws DatastoreException {
      try {
         long id = rs.getLong(1);
         long endpointId = rs.getLong(2);
         long topicId = rs.getLong(3);
         String callbackURL = rs.getString(4);
         Subscription.Builder builder = new Subscription.Builder(id, callbackURL, topicId, endpointId);
         Subscription.Status status = Subscription.Status.fromValue(rs.getInt(5));
         int leaseSeconds = rs.getInt(6);
         String hmacSecret = rs.getString(7);
         long expireTime = rs.getTimestamp(8).getTime();
         builder.setLeaseSeconds(leaseSeconds).setSecret(hmacSecret).setStatus(status).setExpireTime(new Date(expireTime));
         return builder;
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscription", se);
      }
   }

   @Override
   public AuthScheme resolveAuthScheme(final String scheme) throws DatastoreException {

      if(scheme == null) {
         return null;
      }

      if(scheme.equalsIgnoreCase("basic")) {
         return new BasicAuthScheme();
      } else {
         return null;
      }
   }

   @Override
   public RequestBuilder addAuth(final Endpoint endpoint, final RequestBuilder request) throws DatastoreException {
      if(endpoint.getAuthScheme() != null && endpoint.getAuthId() != null) {
         if(endpoint.getAuthScheme() instanceof BasicAuthScheme) {
            return request.addHeader(BasicAuthScheme.AUTH_HEADER, "Basic " + endpoint.getAuthId());
         } else {
            return request;
         }
      } else {
         return request;
      }
   }

   /**
    * The event handler. May be <code>null</code>. Must be set during initialization.
    */
   protected HubDatastore.EventHandler eventHandler;

   /**
    * A logger. May not be <code>null</code>. Must be set during initialization.
    */
   protected Logger logger;


   private static final String getUniqueSubscriptionSQL = getSubscriptionSQL + "topicId=? AND callbackURL=?";

   @Override
   public Subscription getSubscription(final String topicURL, final String callbackURL) throws DatastoreException {
      Topic topic = getTopic(topicURL, false);
      if(topic == null) {
         return null;
      }

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getUniqueSubscriptionSQL);
         stmt.setLong(1, topic.getId());
         stmt.setString(2, callbackURL);
         rs = stmt.executeQuery();
         if(rs.next()) {
            return getSubscription(rs).setTopic(topic).create();
         } else {
            return null;
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscription", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String getSubscriberSQL = "SELECT id, authScheme, authId FROM subscriber WHERE endpointURL=? AND authScheme=? AND authId=?";

   @Override
   public Subscriber getSubscriber(final String endpointURL, final AuthScheme scheme, final String authId,
                                   final boolean create) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getSubscriberSQL);
         stmt.setString(1, endpointURL);
         stmt.setString(2, scheme == null ? "" : scheme.getScheme());
         stmt.setString(3, authId == null ? "" : authId);
         rs = stmt.executeQuery();
         if(rs.next()) {
            long subscriberId = rs.getLong(1);
            return new Subscriber(endpointURL, subscriberId, scheme, authId);
         } else if(!create) {
            return null;
         } else {
            SQLUtil.closeQuietly(conn, stmt, rs);
            conn = null;
            stmt = null;
            rs = null;
            return createSubscriber(endpointURL, scheme, authId);
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscriber", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String getSubscriberIdSQL = "SELECT endpointURL, authScheme, authId FROM subscriber WHERE id=?";

   @Override
   public Subscriber getSubscriber(final long id) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getSubscriberIdSQL);
         stmt.setLong(1, id);
         rs = stmt.executeQuery();
         if(rs.next()) {
            return new Subscriber(rs.getString(1), id, resolveAuthScheme(rs.getString(2)), rs.getString(3));
         } else {
            return null;
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscriber", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String createSubscriberSQL = "INSERT IGNORE INTO subscriber (endpointURL, authScheme, authId) VALUES (?,?,?)";

   @Override
   public Subscriber createSubscriber(final String endpointURL, final AuthScheme scheme, final String authId) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(createSubscriberSQL, Statement.RETURN_GENERATED_KEYS);
         stmt.setString(1, endpointURL);
         stmt.setString(2, scheme == null ? null : scheme.getScheme());
         stmt.setString(3, authId);
         stmt.executeUpdate();
         rs = stmt.getGeneratedKeys();
         if(rs.next()) {
            return new Subscriber(endpointURL, rs.getLong(1), scheme, authId);
         } else {
            SQLUtil.closeQuietly(conn, stmt, rs);
            conn = null;
            stmt = null;
            rs = null;
            return getSubscriber(endpointURL, scheme, authId, false);
         }
      } catch(SQLException se) {
         throw new DatastoreException("Problem creating subscriber", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String getSubscriptionEndpointsSQL = "SELECT DISTINCT callbackHost FROM subscription " +
           "ORDER BY callbackHost ASC LIMIT ?,?";

   @Override
   public List<String> getSubscribedHosts(int start, int limit) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      List<String> hosts = Lists.newArrayListWithExpectedSize(limit < 64 ? limit : 64);
      try {
         conn = getConnection();
         stmt = conn.prepareStatement(getSubscriptionEndpointsSQL);
         stmt.setInt(1, start);
         stmt.setInt(2, limit);
         rs = stmt.executeQuery();
         while(rs.next()) {
            hosts.add(rs.getString(1));
         }
         return hosts;

      } catch(SQLException se) {
         throw new DatastoreException("Problem getting subscribed hosts", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private static final String countActiveHostSubscriptionsSQL = "SELECT COUNT(id) FROM subscription " +
           "WHERE callbackHost=?";

   @Override
   public int countActiveHostSubscriptions(String host) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
         conn = getConnection();
         stmt = conn.prepareStatement(countActiveHostSubscriptionsSQL);
         stmt.setString(1, host);
         rs = stmt.executeQuery();
         return rs.next() ? rs.getInt(1) : 0;
      } catch(SQLException se) {
         throw new DatastoreException("Problem counting active host subscriptions", se);
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }
}