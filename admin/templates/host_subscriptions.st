host_subscriptions(host, subscriptions, activeOnly) ::= <<
<h3>Subscriptions for $host$</h3>
$if(activeOnly)$
<h5 class="subheader"><b>Active</b> &middot; <a href="/admin/host/$host$">All</a></h5>
$else$
<h5 class="subheader"><a href="/admin/host/$host$/active">Active</a> &middot; <b>All</b></h5>
$endif$
<table>
    <thead>
    <tr>
        <th>Topic</th>
        <th>Path</th>
        <th>Status</th>
        <th>Expire Time</th>
    </tr>
    </thead>
    <tbody>
    $subscriptions:{subscription |
    <tr>
        <!-- $subscription.id$ -->
        <td><a href="/admin/topic/$subscription.topic.id$">$subscription.topic.uRL$</a></td>
        <td>$subscription.callbackPath$</td>
        <td>$subscription.status$</td>
        <td>$subscription.expireTime; format="date:long"$</td>
    </tr>
    }$
    </tbody>
</table>
>>