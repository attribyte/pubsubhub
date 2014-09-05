all_subscriptions(subscriptions, activeOnly) ::= <<
<h3>Subscriptions</h3>
$if(activeOnly)$
<h5 class="subheader"><b>Active</b> &middot; <a href="/admin/subscriptions">All</a></h5>
$else$
<h5 class="subheader"><a href="/admin/subscriptions/active">Active</a> &middot; <b>All</b></h5>
$endif$

<table>
    <thead>
    <tr>
        <th>Topic</th>
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
        <td><a href="/admin/topic/$subscription.topic.id$">$subscription.topic.uRL$</a></td>
        <td><a href="/admin/host/$subscription.callbackHost$">$subscription.callbackHost$</a></td>
        <td>$subscription.callbackPath$</td>
        <td><a href="$subscription.callbackURL$" class="ss" id="ss_$subscription.id$" name="$subscription.status$">$subscription.status$</a>&nbsp;
            <a href="/admin/metrics/$subscription.id$"><i class="fi-graph-bar"></i></a>
        </td>
        <td>$subscription.expireTime; format="date:long"$</td>
    </tr>
    }$
    </tbody>
</table>
<script>
    bindSubscriptionEdit();
</script>
>>