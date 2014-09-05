subscribers(subscribers, paging) ::= <<
<h4>Subscribers</h4>
$pagination()$
<table>
    <thead>
    <tr>
        <th>Host</th>
        <th>Subscription Count</th>
    </tr>
    </thead>
    <tbody>
    $subscribers:{subscriber |
    <tr>
        <td><a href="/admin/host/$subscriber.hostLink$">$subscriber.host$</a></td>
        <td>$subscriber.activeSubscriptionCount$</td>
    </tr>
    }$
    </tbody>
</table>
>>