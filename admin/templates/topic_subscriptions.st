topic_subscriptions(topic, subscriptions, activeOnly, paging) ::= <<
<h4>Subscriptions
    <small>$topic.url$</small>
</h4>
$if(activeOnly)$
<h5 class="subheader"><b>Active</b> &middot; <a href="/admin/topic/$topic.id$">All</a></h5>
$else$
<h5 class="subheader"><a href="/admin/topic/$topic.id$/active">Active</a> &middot; <b>All</b></h5>
$endif$
$pagination()$
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
        <td><a href="$subscription.callbackURL$" class="ss" id="ss_$subscription.id$" name="$subscription.status$">$subscription.status$</a>
            &nbsp;
            <a href="/admin/metrics/$subscription.id$"><i class="fi-graph-bar"></i></a>
        </td>
        <td>$subscription.expireTime; format="date:long"$</td>
    </tr>
    }$
    </tbody>
</table>
<script>
    jQuery(window).load(function () {
        bindSubscriptionEdit();
    });
</script>
>>