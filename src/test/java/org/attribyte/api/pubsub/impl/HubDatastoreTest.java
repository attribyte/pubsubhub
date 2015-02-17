package org.attribyte.api.pubsub.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.attribyte.api.DatastoreException;
import org.attribyte.api.InitializationException;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.http.AuthScheme;
import org.attribyte.api.http.GetRequestBuilder;
import org.attribyte.api.http.Header;
import org.attribyte.api.http.RequestBuilder;
import org.attribyte.api.http.impl.BasicAuthScheme;
import org.attribyte.api.pubsub.HubDatastore;
import org.attribyte.api.pubsub.Subscriber;
import org.attribyte.api.pubsub.Subscription;
import org.attribyte.api.pubsub.Topic;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Random;
import java.util.Set;

import java.io.IOException;

/**
 * Tests for datastore implementations.
 * @param <T> The datastore class to be tested.
 */
public abstract class HubDatastoreTest<T extends HubDatastore> {

   private T datastore;
   protected abstract T createDatastore() throws InitializationException;
   private static HubDatastore _datastore;
   private Random rnd = new Random();

   @Before
   public void setUp() throws InitializationException {
      datastore = createDatastore();
      _datastore = datastore;
   }

   @After
   public void tearDown() throws IOException {
      _datastore.shutdown();
   }

   @Test
   public void createTopic() throws DatastoreException {
      String topicURL = randomTopic();
      Topic topic = datastore.getTopic(topicURL, true);
      assertNotNull(topic);
      assertNotNull(topic.getURL());
      assertEquals(topicURL, topic.getURL());
      assertTrue(topic.getId() > 0L);
      assertNotNull(topic.getCreateTime());
   }

   @Test
   public void missingTopic() throws DatastoreException {
      String topicURL = randomTopic();
      Topic topic = datastore.getTopic(topicURL, false);
      assertNull(topic);
   }

   @Test
   public void topicById() throws DatastoreException {
      String topicURL = randomTopic();
      Topic topic = datastore.getTopic(topicURL, true);
      assertNotNull(topic);
      topic = datastore.getTopic(topic.getId());
      assertNotNull(topic);
   }

   @Test
   public void topicList() throws DatastoreException {
      String topicURL0 = randomTopic();
      String topicURL1 = randomTopic();
      String topicURL2 = randomTopic();
      String topicURL3 = randomTopic();
      datastore.getTopic(topicURL0, true);
      datastore.getTopic(topicURL1, true);
      datastore.getTopic(topicURL2, true);
      datastore.getTopic(topicURL3, true);

      List<Topic> topics = datastore.getTopics(0, 4);
      assertNotNull(topics);
      assertEquals(4, topics.size());

      Set<Topic> topicSet = Sets.newHashSet(topics);
      assertEquals(4, topics.size());

      topics = datastore.getTopics(1, 2);
      assertNotNull(topics);
      assertEquals(2, topics.size());
   }

   @Test
   public void activeTopics() throws DatastoreException {
      createSubscriptionMix();
      List<Topic> activeTopics = datastore.getActiveTopics(0, 3);
      assertNotNull(activeTopics);
      assertEquals(3, activeTopics.size());
   }

   @Test
   public void createSubscriber() throws DatastoreException {
      Subscriber subscriber = datastore.createSubscriber("http://127.0.0.1:8087", null, null);
      assertNotNull(subscriber);
      assertTrue(subscriber.getId() > 0);
      assertNull(subscriber.getAuthId());
      assertNull(subscriber.getAuthScheme());
   }

   @Test
   public void createSubscriberOnGet() throws DatastoreException {
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8087", null, null, true);
      assertNotNull(subscriber);
      assertTrue(subscriber.getId() > 0);
      assertNull(subscriber.getAuthId());
      assertNull(subscriber.getAuthScheme());
   }

   @Test
   public void subscriberById() throws DatastoreException {
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8088", null, null, true);
      assertNotNull(subscriber);
      subscriber = datastore.getSubscriber(subscriber.getId());
      assertNotNull(subscriber);
   }

   @Test
   public void createSubscriberWithAuth() throws DatastoreException {
      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8089", scheme, "topsecret", true);
      assertNotNull(subscriber);
      assertTrue(subscriber.getId() > 0);
      assertEquals("topsecret", subscriber.getAuthId());
      assertNotNull(subscriber.getAuthScheme());
   }

   @Test
   public void missingSubscriber() throws DatastoreException {
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8090", null, null, false);
      assertNull(subscriber);
   }

   @Test
   public void createSubscription() throws DatastoreException {
      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8089", scheme, "topsecret", true);
      String callbackURL = "http://127.0.0.1:8089/topic/1";
      Subscription.Builder builder;
      Topic topic = datastore.getTopic("/topic/8089/1", true);
      builder = new Subscription.Builder(0L, callbackURL, topic, subscriber);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      Subscription subscription = datastore.updateSubscription(builder.create(), true); //Extend lease...
      assertNotNull(subscription);
      assertTrue(subscription.getId() > 0L);
      subscription = datastore.getSubscription(subscription.getId());
      assertNotNull(subscription);
      assertEquals("127.0.0.1:8089", subscription.getCallbackHost());
      assertEquals("/topic/1", subscription.getCallbackPath());
      assertEquals("http://127.0.0.1:8089/topic/1", subscription.getCallbackURL());
      assertEquals(subscriber.getId(), subscription.getEndpointId());
      assertEquals(3600, subscription.getLeaseSeconds());
      assertEquals("topsecret", subscription.getSecret());
      assertNotNull(subscription.getTopic());
      assertEquals(topic, subscription.getTopic());
      assertEquals(Subscription.Status.ACTIVE, subscription.getStatus());
      assertNotNull(subscription.getExpireTime());
      assertTrue(subscription.getExpireTime().getTime() > System.currentTimeMillis());
   }

   @Test
   public void changeSubscriptionStatus() throws DatastoreException {
      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8089", scheme, "topsecret", true);
      String callbackURL = "http://127.0.0.1:8089/topic/2";
      Subscription.Builder builder;
      Topic topic = datastore.getTopic("/topic/8089/2", true);
      builder = new Subscription.Builder(0L, callbackURL, topic, subscriber);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      Subscription subscription = datastore.updateSubscription(builder.create(), true); //Extend lease...
      assertEquals(Subscription.Status.ACTIVE, subscription.getStatus());
      assertNotNull(subscription);
      assertTrue(subscription.getId() > 0L);
      datastore.changeSubscriptionStatus(subscription.getId(), Subscription.Status.PENDING, 0);
      subscription = datastore.getSubscription(subscription.getId());
      assertEquals(Subscription.Status.PENDING, subscription.getStatus());
   }

   @Test
   public void expireSubscription() throws DatastoreException {
      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8089", scheme, "topsecret", true);
      String callbackURL = "http://127.0.0.1:8089/topic/2";
      Subscription.Builder builder;
      Topic topic = datastore.getTopic("/topic/8089/2", true);
      builder = new Subscription.Builder(0L, callbackURL, topic, subscriber);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      Subscription subscription = datastore.updateSubscription(builder.create(), true); //Extend lease...
      assertEquals(Subscription.Status.ACTIVE, subscription.getStatus());
      assertNotNull(subscription);
      assertTrue(subscription.getId() > 0L);
      datastore.expireSubscription(subscription.getId());
      subscription = datastore.getSubscription(subscription.getId());
      assertEquals(Subscription.Status.EXPIRED, subscription.getStatus());
   }

   @Test
   public void expireSubscriptions() throws DatastoreException {
      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber = datastore.getSubscriber("http://127.0.0.1:8089", scheme, "topsecret", true);
      String callbackURL = "http://127.0.0.1:8089/topic/2";
      Subscription.Builder builder;
      Topic topic = datastore.getTopic("/topic/8089/2", true);
      builder = new Subscription.Builder(0L, callbackURL, topic, subscriber);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(-100000);
      builder.setSecret("topsecret");
      Subscription subscription = datastore.updateSubscription(builder.create(), true);
      assertEquals(Subscription.Status.ACTIVE, subscription.getStatus());
      datastore.expireSubscriptions(10);
      subscription = datastore.getSubscription(subscription.getId());
      assertEquals(Subscription.Status.EXPIRED, subscription.getStatus());
   }

   @Test
   public void topicSubscriptions() throws DatastoreException {
      createSubscriptionMix();
      Topic topic1 = datastore.getTopic("/mix/1", true);
      Set<Subscription.Status> status = ImmutableSet.of(Subscription.Status.ACTIVE);
      List<Subscription> subscriptions = datastore.getTopicSubscriptions(topic1, status, 0, 100);
      assertNotNull(subscriptions);
      assertEquals(3, subscriptions.size());
      subscriptions = datastore.getTopicSubscriptions(topic1, status, 1, 100);
      assertEquals(2, subscriptions.size());

      status = ImmutableSet.of(Subscription.Status.REMOVED);
      subscriptions = datastore.getTopicSubscriptions(topic1, status, 0, 100);
      assertEquals(0, subscriptions.size());

      Topic topic2 = datastore.getTopic("/mix/2", true);
      subscriptions = datastore.getTopicSubscriptions(topic2, status, 0, 100);
      assertEquals(1, subscriptions.size());
   }

   @Test
   public void activeSubscriptions() throws DatastoreException {
      createSubscriptionMix();
      Topic topic1 = datastore.getTopic("/mix/1", true);
      List<Subscription> subscriptions = Lists.newArrayListWithCapacity(64);
      long nextId = datastore.getActiveSubscriptions(topic1, subscriptions, 0, 5);
      assertEquals(3, subscriptions.size());
      assertEquals(HubDatastore.LAST_ID, nextId);
      long nextNextId = subscriptions.get(2).getId();
      subscriptions.clear();
      nextId = datastore.getActiveSubscriptions(topic1, subscriptions, 0, 2);
      assertEquals(2, subscriptions.size());
      assertEquals(nextNextId, nextId);
   }

   @Test
   public void hasActiveSubscriptions() throws DatastoreException {
      createSubscriptionMix();
      Topic topic1 = datastore.getTopic("/mix/1", true);
      Topic topic2 = datastore.getTopic(randomTopic(), true);
      assertTrue(datastore.hasActiveSubscriptions(topic1.getId()));
      assertTrue(datastore.hasActiveSubscriptions("http://127.0.0.1:7001/topic/1"));
      assertFalse(datastore.hasActiveSubscriptions(topic2.getId()));
   }

   @Test
   public void countActiveSubscriptions() throws DatastoreException {
      createSubscriptionMix();
      Topic topic1 = datastore.getTopic("/mix/1", true);
      Topic topic2 = datastore.getTopic(randomTopic(), true);
      assertEquals(3, datastore.countActiveSubscriptions(topic1.getId()));
      assertEquals(0, datastore.countActiveSubscriptions(topic2.getId()));
   }

   @Test
   public void subscribedHosts() throws DatastoreException {
      createSubscriptionMix();
      createSubscriptionMix();
      List<String> hosts = datastore.getSubscribedHosts(0, 3);
      assertEquals(3, hosts.size());
   }

   @Test
   public void activeHostSubscriptions() throws DatastoreException {
      createSubscriptionMix();
      assertEquals(1, datastore.countActiveHostSubscriptions("127.0.0.1:7001"));
   }

   @Test
   public void subscriptionState() throws DatastoreException {
      createSubscriptionMix();
      Topic topic1 = datastore.getTopic("/mix/1", true);
      org.attribyte.api.pubsub.SubscriptionState state = datastore.getSubscriptionState(topic1);
      assertNotNull(state);
   }

   @Test
   public void authHeader() throws DatastoreException {
      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber = datastore.createSubscriber("http://127.0.0.1:8089", scheme, "topsecret");
      Header header = datastore.getAuthHeader(subscriber);
      assertNotNull(header);
      assertEquals("Authorization=Basic topsecret", header.toString());
   }

   @Test
   public void addRequestAuth() throws DatastoreException, InvalidURIException {
      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber = datastore.createSubscriber("http://127.0.0.1:8089", scheme, "topsecret");
      RequestBuilder builder = new GetRequestBuilder("http://127.0.0.1");
      datastore.addAuth(subscriber, builder);
      Header header = builder.create().getHeader("Authorization");
      assertNotNull(header);
      assertEquals("Basic topsecret", header.getValue());
   }

   @Test
   public void resolveAuthScheme() throws DatastoreException {
      AuthScheme basicAuthScheme = datastore.resolveAuthScheme("Basic");
      assertNotNull(basicAuthScheme);
      assertEquals("Basic", basicAuthScheme.getScheme());
   }

   private String randomTopic() {
      return "/" + Integer.toString(rnd.nextInt(5000));
   }

   private void createSubscriptionMix() throws DatastoreException {

      AuthScheme scheme = new BasicAuthScheme();
      Subscriber subscriber0 = datastore.getSubscriber("http://127.0.0.1:7000", scheme, "topsecret0", true);
      Subscriber subscriber1 = datastore.getSubscriber("http://127.0.0.1:7001", scheme, "topsecret1", true);
      Subscriber subscriber2 = datastore.getSubscriber("http://127.0.0.1:7002", scheme, "topsecret2", true);
      String callbackURL0 = "http://127.0.0.1:7000/topic/1";
      String callbackURL1 = "http://127.0.0.1:7001/topic/1";
      String callbackURL2 = "http://127.0.0.1:7002/topic/1";

      Topic topic0 = datastore.getTopic("/mix/0", true);
      Topic topic1 = datastore.getTopic("/mix/1", true);
      Topic topic2 = datastore.getTopic("/mix/2", true);


      Subscription.Builder builder;

      builder = new Subscription.Builder(0L, callbackURL0, topic0, subscriber0);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      datastore.updateSubscription(builder.create(), true);

      builder = new Subscription.Builder(0L, callbackURL0, topic1, subscriber0);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      datastore.updateSubscription(builder.create(), true);

      builder = new Subscription.Builder(0L, callbackURL0, topic2, subscriber0);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      datastore.updateSubscription(builder.create(), true);

      builder = new Subscription.Builder(0L, callbackURL1, topic1, subscriber1);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      datastore.updateSubscription(builder.create(), true);

      builder = new Subscription.Builder(0L, callbackURL1, topic2, subscriber1);
      builder.setStatus(Subscription.Status.REMOVED);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      datastore.updateSubscription(builder.create(), true);

      builder = new Subscription.Builder(0L, callbackURL2, topic1, subscriber2);
      builder.setStatus(Subscription.Status.ACTIVE);
      builder.setLeaseSeconds(3600);
      builder.setSecret("topsecret");
      datastore.updateSubscription(builder.create(), true);
   }
}