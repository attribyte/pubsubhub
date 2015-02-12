package org.attribyte.api.pubsub.impl;

import org.attribyte.api.DatastoreException;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.util.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/**
 * A datastore implementation that uses MySQL syntax.
 */
public class MySQLDatastore extends RDBDatastore {

   private static final String getTopicSQL = "SELECT id, createTime FROM topic WHERE topicURL=?";
   private static final String createTopicSQL = "INSERT IGNORE INTO topic (topicURL, topicHash) VALUES (?, MD5(?))";

   @Override
   public Topic getTopic(final String topicURL, final boolean create) throws DatastoreException {

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

   private static final String updateSubscriptionSQL = "UPDATE subscription SET endpointId=?, status=?, leaseSeconds=?, hmacSecret=? WHERE id=?";
   private static final String updateSubscriptionExtendLeaseSQL =
           "UPDATE subscription SET endpointId=?, status=?, leaseSeconds=?, hmacSecret=?, expireTime=NOW() + INTERVAL ? SECOND WHERE id=?";
   private static final String createSubscriptionSQL =
           "INSERT INTO subscription (endpointId, topicId, callbackURL, callbackHash, callbackHost, callbackPath, status, createTime, leaseSeconds, hmacSecret, expireTime)" +
                   "VALUES (?,?,?,MD5(?),?,?,?,NOW(),?,?,NOW()+INTERVAL ? SECOND) ON DUPLICATE KEY UPDATE endpointId=?, status=?, leaseSeconds=?, hmacSecret=?, expireTime=NOW()+INTERVAL ? SECOND";

   @Override
   public Subscription updateSubscription(final Subscription subscription, boolean extendLease) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      final Subscription currSubscription;
      if(subscription.getId() > 0L) {
         currSubscription = getSubscription(subscription.getId());
      } else {
         currSubscription = getSubscription(subscription.getTopic().getURL(), subscription.getCallbackURL());
      }

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
               stmt.setLong(5, currSubscription.getId());
            } else {
               stmt.setInt(5, subscription.getLeaseSeconds());
               stmt.setLong(6, currSubscription.getId());
            }
         }

         stmt.executeUpdate();
         SQLUtil.closeQuietly(conn, stmt); //Avoid using two connections concurrently...
         conn = null;
         stmt = null;
         rs = null;

         return getSubscription(currSubscription.getId());

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
   public void changeSubscriptionStatus(final long id, final Subscription.Status newStatus, final int newLeaseSeconds) throws DatastoreException {

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
}
