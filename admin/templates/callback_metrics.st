callback_metrics(metrics) ::= <<
<h3>Callback Metrics</h3>
<table>
    <thead>
    <tr>
        <th>Host</th>
        <th>Count</th>
        <th>Callback/s</th>
        <th>95% (ms)</th>
        <th>Failed</th>
        <th>Failed/s</th>
        <th>Abandoned</th>
    </tr>
    </thead>
    <tbody>
    $metrics:{metric |
    <tr>
        <td><a href="/admin/metrics/$metric.host$">$metric.host$</a></td>
        <td>$metric.count$</td>
        <td>$metric.rate$</td>
        <td>$metric.timing$</td>
        <td>$metric.failedCount$</td>
        <td>$metric.failedRate$</td>
        <td>$metric.abandonedCount$</td>
    </tr>
    }$
    </tbody>
</table>
>>