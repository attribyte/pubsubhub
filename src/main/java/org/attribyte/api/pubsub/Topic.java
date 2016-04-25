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

import org.attribyte.util.StringUtil;

import java.util.Date;

/**
 * A resource, identified by a URL, to which subscriptions are made.
 * <p>
 *    Topics are uniquely identified by their URL and optionally have an
 *    (also unique) assigned integer id.
 * </p>
 * @author Attribyte, LLC
 */
public final class Topic {

   /**
    * The distribution topology.
    */
   public enum Topology {
      /**
       * Broadcast to all subscribers (the default).
       */
      BROADCAST(0),

      /**
       * Broadcast to a single subscriber.
       */
      SINGLE_SUBSCRIBER(1);

      Topology(final int id) {
         this.id = id;
      }

      /**
       * Gets the topology id.
       * @return The id.
       */
      public final int getId() {
         return id;
      }

      /**
       * The topology id.
       */
      public final int id;
   }

   /**
    * Creates immutable instances of <code>Topic</code>.
    */
   public static final class Builder {

      /**
       * Sets the id of the topic to be created.
       * @param id The id.
       * @return A self-reference.
       */
      public Builder setId(final long id) {
         this.id = id;
         return this;
      }

      /**
       * Sets the topic URL.
       * @param topicURL The URL.
       * @return A self-reference.
       */
      public Builder setTopicURL(final String topicURL) {
         this.topicURL = topicURL;
         return this;
      }

      /**
       * Sets the create time.
       * @param createTime The create time.
       * @return A self-reference.
       */
      public Builder setCreateTime(final Date createTime) {
         this.createTime = createTime;
         return null;
      }

      /**
       * Creates the topic.
       * @return The topic.
       */
      public Topic create() {
         return new Topic(topicURL, id, createTime, topology);
      }

      private long id;
      private String topicURL;
      private Date createTime = null;
      private Topology topology = Topology.BROADCAST;
   }

   /**
    * Creates a topic with 'broadcast' distribution topology.
    * @param topicURL The URL.
    * @param id The topic id.
    * @param createTime The create time, if known. May be <code>null</code>.
    */
   public Topic(final String topicURL, final long id, final Date createTime) {
      this(topicURL, id, createTime, Topology.BROADCAST);
   }


   /**
    * Creates a topic.
    * @param topicURL The URL.
    * @param id The topic id.
    * @param createTime The create time, if known. May be <code>null</code>.
    * @param topology The distribution topology.
    */
   public Topic(final String topicURL, final long id, final Date createTime, final Topology topology) {
      if(!StringUtil.hasContent(topicURL)) {
         throw new UnsupportedOperationException("Topic URL must not be null or empty");
      }
      this.id = id;
      this.topicURL = topicURL;
      this.createTime = createTime;
      this.topology = topology;
   }

   /**
    * Gets the unique id assigned to the topic.
    * @return The id, if assigned, otherwise <code>0</code>.
    */
   public long getId() {
      return id;
   }

   /**
    * Gets the topic URL.
    * @return The URL.
    */
   public String getURL() {
      return topicURL;
   }

   /**
    * Gets the create time.
    * @return The create time or <code>null</code> if unknown.
    */
   public Date getCreateTime() {
      return createTime;
   }

   /**
    * Gets the distribution topology.
    * @return The topology.
    */
   public Topology getTopology() {
      return topology;
   }

   @Override
   public int hashCode() {
      return topicURL.hashCode();
   }

   @Override
   /**
    * Topics are equal if their URLs match.
    * @param other The object for comparison.
    * @return Are the topic URLs equal?
    */
   public boolean equals(final Object other) {
      if(other instanceof Topic) {
         Topic otherTopic = (Topic)other;
         return otherTopic.topicURL.equals(topicURL);
      } else {
         return false;
      }
   }

   protected final long id;
   protected final String topicURL;
   protected final Date createTime;
   protected final Topology topology;
}