metrics_detail(metrics) ::= <<
<h4>Callback Metrics Detail
    <small>$metrics.name$</small>
</h4>
<h6>Callbacks</h6>
<table>
    <tbody>
    $display_metered(metrics.callbacks)$
    $display_timer(metrics.callbacks)$
    </tbody>
</table>

<h6>Time to Callback Complete</h6>
<table>
    <tbody>
    $display_timer(metrics.timeToCallback)$
    </tbody>
</table>


<h6>Failed Callbacks</h6>
<table>
    <tbody>
    $display_metered(metrics.failedCallbacks)$
    </tbody>
</table>

<h6>Abandoned Callbacks</h6>
<table>
    <tbody>
    $display_metered(metrics.abandonedCallbacks)$
    </tbody>
</table>
>>