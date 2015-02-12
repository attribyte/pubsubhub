package org.attribyte.api.pubsub.impl;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InitializationException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.api.pubsub.Topic;
import org.attribyte.api.pubsub.impl.server.util.ServerUtil;
import org.attribyte.util.SQLUtil;
import org.h2.tools.RunScript;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

/**
 * A datastore implementation that uses H2 syntax.
 */
public class H2Datastore extends RDBDatastore {

   public static final String H2_INIT_FILE = "config/h2_pubsub_hub.sql";

   @Override
   public void init(String prefix, Properties props, HubDatastore.EventHandler eventHandler, Logger logger) throws InitializationException {
      super.init(prefix,props, eventHandler, logger);
      String h2InitFile = props.getProperty("h2.initFile", H2_INIT_FILE);
      logger.info("Creating H2 database and tables...");
      File sqlInitFile = new File(ServerUtil.systemInstallDir(), h2InitFile);
      if(!sqlInitFile.exists()) {
         throw new InitializationException("The " + sqlInitFile.getAbsolutePath() + " must exist to use H2");
      }
      if(!sqlInitFile.canRead()) {
         throw new InitializationException("The " + sqlInitFile.getAbsolutePath() + " must be readable to use H2");
      }

      Connection conn = null;

      try {
         conn = getConnection();
         RunScript.execute(conn, new FileReader(sqlInitFile));
      } catch(SQLException se) {
         throw new InitializationException("Problem initializing H2", se);
      } catch(IOException ioe) {
         throw new InitializationException("Problem reading the H2 init file", ioe);
      } finally {
         SQLUtil.closeQuietly(conn);
      }
   }

   private static final String getTopicSQL = "SELECT id, createTime FROM topic WHERE topicURL=?";
   private static final String createTopicSQL = "INSERT INTO topic (topicURL, topicHash) VALUES (?,?)";

   /**
    * Safe to assume (I hope!) that for H2 our process is the only one manipulating the database.
    */
   private final Object CREATE_TOPIC_LOCK = new Object();

   /**
    * H2 does not support the MD5 function.
    */
   private final HashFunction md5 = Hashing.md5();

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
            SQLUtil.closeQuietly(rs);
            rs = null;
            synchronized(CREATE_TOPIC_LOCK) {
               rs = stmt.executeQuery(); //Was the topic created while we were waiting for this lock?
               if(rs.next()) {
                  return new Topic(topicURL, rs.getLong(1), new Date(rs.getTimestamp(2).getTime()));
               }
               SQLUtil.closeQuietly(stmt, rs);
               rs = null;
               stmt = null;

               stmt = conn.prepareStatement(createTopicSQL, Statement.RETURN_GENERATED_KEYS);
               stmt.setString(1, topicURL);
               stmt.setString(2, md5.hashString(topicURL).toString());
               stmt.executeUpdate();
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
           "UPDATE subscription SET endpointId=?, status=?, leaseSeconds=?, hmacSecret=?, expireTime=DATEADD('SECOND', ?, NOW()) WHERE id=?";
   private static final String createSubscriptionSQL =
           "INSERT INTO subscription (endpointId, topicId, callbackURL, callbackHash, callbackHost, callbackPath, status, createTime, leaseSeconds, hmacSecret, expireTime)" +
                   "VALUES (?,?,?,?,?,?,?,NOW(),?,?,DATEADD('SECOND', ?, NOW()))";

   /**
    * Assume for H2 that this is the only process manipulating the database.
    */
   private final Object UPDATE_SUBSCRIPTION_LOCK = new Object();

   @Override
   public Subscription updateSubscription(final Subscription subscription, boolean extendLease) throws DatastoreException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      synchronized(UPDATE_SUBSCRIPTION_LOCK) {

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
               stmt.setString(4, md5.hashString(subscription.getCallbackURL()).toString());
               stmt.setString(5, subscription.getCallbackHost());
               stmt.setString(6, subscription.getCallbackPath());
               stmt.setInt(7, subscription.getStatus().getValue());
               stmt.setInt(8, subscription.getLeaseSeconds() < 0 ? 0 : subscription.getLeaseSeconds());
               stmt.setString(9, subscription.getSecret());
               stmt.setInt(10, subscription.getLeaseSeconds());
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
   }

   private static final String changeSubscriptionStatusSQL = "UPDATE subscription SET status=? WHERE id=?";
   private static final String changeSubscriptionStatusLeaseSQL =
           "UPDATE subscription SET status=?, leaseSeconds=?, expireTime=DATEADD('SECOND', ?, NOW()) WHERE id=?";

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

   private static final String createSubscriberSQL = "INSERT INTO subscriber (endpointURL, authScheme, authId) VALUES (?,?,?)";

   /**
    * Assume for H2 that this is the only process manipulating the database.
    */
   private final Object CREATE_SUBSCRIBER_LOCK = new Object();

   @Override
   public Subscriber createSubscriber(final String endpointURL, final AuthScheme scheme, final String authId) throws DatastoreException {
      synchronized(CREATE_SUBSCRIBER_LOCK) {
         //Was the subscriber created by another thread while we were waiting for this lock?
         Subscriber subscriber = getSubscriber(endpointURL, scheme, authId, false);
         if(subscriber != null) {
            return subscriber;
         }

         Connection conn = null;
         PreparedStatement stmt = null;
         ResultSet rs = null;
         try {
            conn = getConnection();
            stmt = conn.prepareStatement(createSubscriberSQL, Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, endpointURL);
            stmt.setString(2, scheme == null ? "" : scheme.getScheme());
            stmt.setString(3, authId == null ? "" : authId);
            stmt.executeUpdate();
            rs = stmt.getGeneratedKeys();
            if(rs.next()) {
               return new Subscriber(endpointURL, rs.getLong(1), scheme, authId);
            } else {
               throw new DatastoreException("Problem creating subscriber: Expecting 'id' to be generated.");
            }
         } catch(SQLException se) {
            throw new DatastoreException("Problem creating subscriber", se);
         } finally {
            SQLUtil.closeQuietly(conn, stmt, rs);
         }
      }
   }
}
