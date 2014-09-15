notification_metrics_detail(metrics) ::= <<
<h4>Notification Metrics Detail
    <small>$metrics.name$</small>
</h4>
<h6>Notifications</h6>
<table>
    <tbody>
    $display_metered(metrics.notifications)$
    $display_timer(metrics.notifications)$
    </tbody>
</table>

<h6>Notification Size</h6>
<table>
    <tbody>
    $display_histogram(metrics.size)$
    </tbody>
</table>
>>