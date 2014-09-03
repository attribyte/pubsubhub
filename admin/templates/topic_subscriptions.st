topic_subscriptions(topic, subscriptions, activeOnly) ::= <<
<h3>Subscriptions for $topic.url$</h3>
$if(activeOnly)$
<h5 class="subheader"><b>Active</b> &middot; <a href="/admin/topic/$topic.id$">All</a></h5>
$else$
<h5 class="subheader"><a href="/admin/topic/$topic.id$/active">Active</a> &middot; <b>All</b></h5>
$endif$

<table>
    <thead>
    <tr>
        <th>Host</th>
        <th>Path</th>
        <th>Status</th>
        <th>Expire Time</th>
    </tr>
    </thead>
    <tbody>
    $subscriptions:{subscription |
    <tr>
        <!-- $subscription.id$ -->
        <td><a href="/admin/host/$subscription.callbackHost$">$subscription.callbackHost$</a></td>
        <td>$subscription.callbackPath$</td>
        <td>$subscription.status$</td>
        <td>$subscription.expireTime; format="date:long"$</td>
    </tr>
    }$
    </tbody>
</table>

>>