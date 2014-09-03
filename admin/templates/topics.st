topics(topics, activeOnly) ::= <<
<h3>Topics</h3>
$if(activeOnly)$
<h5 class="subheader"><b>Active</b> &middot; <a href="/admin/topics">All</a></h5>
$else$
<h5 class="subheader"><a href="/admin/topics/active">Active</a> &middot; <b>All</b></h5>
$endif$

<table>
    <thead>
    <tr>
        <th>Topic</th>
        <th>Active Subscriptions</th>
        <th>Create Time</th>
    </tr>
    </thead>
    <tbody>
    $topics:{topic |
    <tr>
        <!-- $topic.id$ -->
        $if(topic.activeSubscriptions)$
        <td><a href="/admin/topic/$topic.id$">$topic.url$</a></td>
        $else$
        <td>$topic.url$</td>
        $endif$
        <td>$topic.activeSubscriptionCount$</td>
        <td>$topic.createTime; format="date:long"$</td>
    </tr>
    }$
    </tbody>
</table>
>>