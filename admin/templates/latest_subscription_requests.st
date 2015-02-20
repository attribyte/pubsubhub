latest_subscription_requests(subscriptions) ::= <<
<h4>Recent Subscription Requests</h4>
$subscriptions:{subscription |
 <h5 class="subheader $if(subscription.failed)$err$endif$">$subscription.createTime; format="yyyy-MM-dd HH:mm:ss.SSS"$
  $subscription.sourceIP$ -&gt; $subscription.topicURL$ ($subscription.responseCode$) &lt;- $subscription.callbackURL$
 </h5>
 $if(subscription.bodyHasContent)$
 <p><code>$notification.bodyHTML$</code></p>
 $endif$
}$
>>