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
       * Creates the topic.
       * @return The topic.
       */
      public Topic create() {
         return new Topic(topicURL, id);
      }
      
      private long id;
      private String topicURL;
   }
   
   /**
    * Creates a topic.
    * @param topicURL The URL.
    * @param id The topic id.
    */
   public Topic(final String topicURL, final long id) {
      if(!StringUtil.hasContent(topicURL)) {
         throw new UnsupportedOperationException("Topic URL must not be null or empty");
      }      
      this.id = id;
      this.topicURL = topicURL;
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
   public boolean equals(Object other) {
      if(other instanceof Topic) {
         Topic otherTopic = (Topic)other;
         return otherTopic.topicURL.equals(topicURL);         
      } else {
         return false;
      }
   }
   
   protected final long id;
   protected final String topicURL;
}
