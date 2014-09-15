package org.attribyte.api.pubsub.impl.server.admin;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.codec.binary.Base64;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class AdminAuth {

   public AdminAuth(final String realm, final char[] username, final char[] password) {
      this.realm = realm;
      this.expectedHeaderHash = buildAuthHeaderValue(username, password);
      this.authenticationRequiredHeader = "Basic realm=\"" + realm + "\"";
   }

   boolean authIsValid(HttpServletRequest request, HttpServletResponse response) throws IOException {
      boolean authIsValid = authIsValid(request.getHeader(AUTH_HEADER));
      if(!authIsValid) {
         response.setHeader(MUST_AUTHENTICATE_HEADER, authenticationRequiredHeader);
         response.setStatus(401);
      }
      return authIsValid;
   }

   private HashCode buildAuthHeaderValue(final char[] username, final char[] password) {
      StringBuilder buf = new StringBuilder();
      buf.append(username);
      buf.append(":");
      buf.append(password);
      String up = buf.toString();
      byte[] upBytes = Base64.encodeBase64(up.getBytes());
      buf.setLength(0);
      buf.append("Basic ");
      buf.append(new String(upBytes, Charsets.US_ASCII));
      return hashFunction.hashString(buf.toString());
   }

   private boolean authIsValid(final String headerValue) {
      return headerValue != null && expectedHeaderHash.equals(hashFunction.hashString(headerValue));
   }

   final String realm;
   final HashCode expectedHeaderHash;
   final String authenticationRequiredHeader;


   private static final String AUTH_HEADER = "Authorization";
   private static final String MUST_AUTHENTICATE_HEADER = "WWW-Authenticate";
   private static HashFunction hashFunction = Hashing.md5();
}