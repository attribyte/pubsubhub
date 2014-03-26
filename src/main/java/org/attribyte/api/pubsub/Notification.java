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

import com.google.common.collect.ImmutableList;
import org.attribyte.api.http.Header;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

/**
 * A notification is pushed to all subscribers when a topic is updated.
 * @author Attribyte, LLC
 */
public class Notification {
   
   /**
    * Creates a notification with content specified as a byte array.
    * @param topic The topic.
    * @param headers The headers to be added.
    * @param content The content.
    */
   public Notification(final Topic topic, final Collection<Header> headers, final byte[] content) {
      this.topic = topic;
      if(headers != null) {
         this.headers = ImmutableList.<Header>copyOf(headers);
      } else {
         this.headers = Collections.emptyList();
      }
      this.content = ByteBuffer.wrap(content).asReadOnlyBuffer();
   }
   
   /**
    * Creates a notification with content specified as a <code>ByteBuffer</code>.
    * @param topic The topic.
    * @param headers The headers to be added.
    * @param content The content.
    */
   public Notification(final Topic topic, final Collection<Header> headers, final ByteBuffer content) {
      this.topic = topic;
      if(headers != null) {
         this.headers = ImmutableList.<Header>copyOf(headers);
      } else {
         this.headers = Collections.emptyList();
      }
      if(content.isReadOnly()) {
         this.content = content;
      } else {
         this.content = content.asReadOnlyBuffer();
      }
   }   
   
   /**
    * Gets the topic.
    * @return The topic.
    */
   public Topic getTopic() {
      return topic;
   }
   
   /**
    * Gets the notification headers.
    * @return The headers.
    */
   public Collection<Header> getHeaders() {
      return headers;
   }
   
   /**
    * Gets the notification content.
    * @return The content.
    */
   public ByteBuffer getContent() {
      return content;
   }
   
   private final Topic topic;
   private final Collection<Header> headers;
   private final ByteBuffer content;
}
