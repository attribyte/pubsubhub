package org.attribyte.api.pubsub.impl.client;

import com.google.common.base.Charsets;
import org.apache.commons.codec.binary.Base64;

/**
 * Holds HTTP 'Basic' auth information.
 */
public class BasicAuth {

   public static String AUTH_HEADER_NAME = "Authorization";

   /**
    * Creates basic auth with username and password.
    * @param username The username.
    * @param password The password.
    */
   public BasicAuth(final String username, final String password) {
      this.username = username;
      this.password = password;
      headerValue = buildAuthHeaderValue();
   }

   /**
    * The header value.
    */
   final String headerValue;

   /**
    * The username.
    */
   final String username;

   /**
    * The password.
    */
   final String password;

   /**
    * Builds the auth header value.
    * @return The auth header value.
    */
   private String buildAuthHeaderValue() {
      StringBuilder buf = new StringBuilder(username.trim());
      buf.append(":");
      buf.append(password.trim());
      String up = buf.toString();
      buf.setLength(0);

      byte[] bytes = Base64.encodeBase64(up.getBytes());
      buf.append("Basic ");
      buf.append(new String(bytes, Charsets.UTF_8));
      return buf.toString();
   }
}