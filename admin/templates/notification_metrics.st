notification_metrics(metrics) ::= <<
<h3>Notification Metrics</h3>
<table>
    <thead>
    <tr>
        <th>Topic</th>
        <th>Count</th>
        <th>Notification/s</th>
        <th>95% (ms)</th>
        <th>Mean Size (bytes)</th>
    </tr>
    </thead>
    <tbody>
    $metrics:{metric |
    <tr>
        <td><a href="/admin/nmetrics/$metric.topicId$">$metric.topicName$</a></td>
        <td>$metric.count$</td>
        <td>$metric.rate$</td>
        <td>$metric.timing$</td>
        <td>$metric.meanSize$</td>
    </tr>
    }$
    </tbody>
</table>
>>