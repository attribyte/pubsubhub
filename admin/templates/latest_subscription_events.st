latest_subscription_events(events) ::= <<
<h4>Recent Subscription Events</h4>
$events:{event |
$if(event.verify)$
 $if(event.failed)$
 <h5 class="subheader err">$event.createTime; format="yyyy-MM-dd HH:mm:ss.SSS"$ Verify - $event.callbackURL$
     - Failed Attempts: $event.attempts$
     $if(event.codeIsSet)$($event.callbackResponseCode$)$endif$
     $if(event.abandoned)$ - Abandoned$endif$
 </h5>
  $if(event.reason)$
  <p><code>$event.reason$</code></p>
  $endif$
 $else$
 <h5 class="subheader">$event.createTime; format="yyyy-MM-dd HH:mm:ss.SSS"$ Verify - $event.callbackURL$ - Success</h5>
 $endif$
$else$
 <h5 class="subheader $if(event.failed)$err$endif$">$event.createTime; format="yyyy-MM-dd HH:mm:ss.SSS"$
  Request - $event.sourceIP$ -&gt; $event.topicURL$ ($event.responseCode$) &lt;- $event.callbackURL$
 </h5>
 $if(subscription.bodyHasContent)$
 <p><code>$notification.bodyHTML$</code></p>
 $endif$
$endif$
}$
>>