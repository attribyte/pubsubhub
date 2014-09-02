topics(topics) ::= <<
<h4>Topics</h4>
<table>
    <thead>
    <tr>
        <th>Topic</th>
        <th>Id</th>
        <th>Active Subscriptions</th>
        <th>Create Time</th>
    </tr>
    </thead>
    <tbody>
    $topics:{topic |
    <tr>
        <td><a href="/admin/topic/$topic.id$">$topic.url$</a></td>
        <td><a href="/admin/topic/$topic.id$">$topic.id$</a></td>
        <td>$topic.activeSubscriptionCount$</td>
        <td>$topic.createTime$</td>
    </tr>
    }$
    </tbody>
</table>
>>