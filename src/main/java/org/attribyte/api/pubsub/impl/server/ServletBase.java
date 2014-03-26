package org.attribyte.api.pubsub.impl.server;

import org.attribyte.api.http.Response;
import org.attribyte.api.http.ResponseBuilder;

import javax.servlet.http.HttpServlet;

public abstract class ServletBase extends HttpServlet {

   /**
    * The response sent when the topic for broadcast does not exist.
    */
   protected static final Response ACCEPTED_RESPONSE =
           new ResponseBuilder(Response.Code.ACCEPTED, "").create();

   /**
    * The response sent when the topic for broadcast does not exist.
    */
   protected static final Response UNKNOWN_TOPIC_RESPONSE =
           new ResponseBuilder(Response.Code.BAD_REQUEST, "The topic does not exist").create();

   /**
    * The response sent when the topic was not specified.
    */
   protected static final Response NO_TOPIC_RESPONSE =
           new ResponseBuilder(Response.Code.BAD_REQUEST, "A topic must be specified").create();

   /**
    * The response sent when a notification exceeds that maximum allowed size.
    */
   protected static final Response NOTIFICATION_TOO_LARGE =
           new ResponseBuilder(Response.Code.BAD_REQUEST, "The notification is too large").create();

   /**
    * The response sent on internal server errors.
    */
   protected static final Response INTERNAL_ERROR_RESPONSE =
           new ResponseBuilder(Response.Code.SERVER_ERROR, "The hub is currently unable to accept broadcast requests").create();

}
