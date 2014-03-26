package org.attribyte.api.pubsub.impl.client;

import org.attribyte.api.http.impl.BasicAuthScheme;

/**
 * Holds HTTP 'Basic' auth information.
 */
public class BasicAuth {

   public static String AUTH_HEADER_NAME = BasicAuthScheme.AUTH_HEADER;

   /**
    * Creates basic auth with username and password.
    * @param username The username.
    * @param password The password.
    */
   public BasicAuth(final String username, final String password) {
      this.username = username;
      this.password = password;
      headerValue = BasicAuthScheme.buildAuthHeaderValue(username, password);
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
}