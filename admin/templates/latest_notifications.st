latest_notifications(notifications) ::= <<
    <h4>Recent Notifications</h4>
    $notifications:{notification |
        <h5 class="subheader $if(notification.failed)$err$endif$">$notification.createTime; format="yyyy-MM-dd HH:mm:ss.SSS"$
        $notification.sourceIP$ -&gt; $notification.topicURL$ ($notification.responseCode$)</h5>
        <p><code>$notification.bodyHTML$</code></p>
    }$
>>