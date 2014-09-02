host_subscriptions(host, subscriptions) ::= <<
<h4>Subscriptions for $host$</h4>
<table>
    <thead>
    <tr>
        <th>Id</th>
        <th>Topic</th>
        <th>Path</th>
        <th>Status</th>
        <th>Expire Time</th>
    </tr>
    </thead>
    <tbody>
    $subscriptions:{subscription |
    <tr>
        <td>$subscription.id$</td>
        <td><a href="/admin/topic/$subscription.topic.id$">$subscription.topic.uRL$</a></td>
        <td>$subscription.callbackPath$</td>
        <td>$subscription.status$</td>
        <td>$subscription.expireTime$</td>
    </tr>
    }$
    </tbody>
</table>
>>