all_subscriptions(subscriptions) ::= <<
<h4>Subscriptions</h4>
<table>
    <thead>
    <tr>
        <th>Id</th>
        <th>Host</th>
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
        <td><a href="/admin/host/$subbscription.callbackHost$">$subscription.callbackHost$</a></td>
        <td>$subscription.callbackPath$</td>
        <td>$subscription.status$</td>
        <td>$subscription.expireTime$</td>
    </tr>
    }$
    </tbody>
</table>
>>