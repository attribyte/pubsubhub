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
import com.google.protobuf.ByteString;
import org.attribyte.api.http.Header;

import java.util.Collection;
import java.util.Collections;

/**
 * A notification is pushed to all subscribers when a topic is updated.
 * @author Attribyte, LLC
 */
public class Notification {

   /**
    * Creates a notification with content specified as a <code>ByteString</code>.
    * @param topic The topic.
    * @param headers The headers to be added.
    * @param content The content.
    */
   public Notification(final Topic topic, final Collection<Header> headers, final ByteString content) {
      this.topic = topic;
      if(headers != null) {
         this.headers = ImmutableList.copyOf(headers);
      } else {
         this.headers = Collections.emptyList();
      }
      this.content = content;
   }

   /**
    * Creates a notification with content specified as bytes.
    * @param topic The topic.
    * @param headers The headers to be added.
    * @param content The content.
    */
   public Notification(final Topic topic, final Collection<Header> headers, final byte[] content) {
      this.topic = topic;
      if(headers != null) {
         this.headers = ImmutableList.copyOf(headers);
      } else {
         this.headers = Collections.emptyList();
      }
      this.content = ByteString.copyFrom(content);
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
   public ByteString getContent() {
      return content;
   }

   private final Topic topic;
   private final Collection<Header> headers;
   private final ByteString content;
}
