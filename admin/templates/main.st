main(content) ::= <<
$head()$
<body>
<div class="row">
    <div class="large-3 columns">
        <h1><img src="http://placehold.it/400x100&text=Logo"/></h1>
    </div>
    <div class="large-9 columns">
        <ul class="inline-list right">
            <li><a href="/admin">Manage Subscriptions</a></li>
            <li><a href="/admin/metrics">Metrics</a></li>
        </ul>
    </div>
</div>
<div class="row">
    <div id="content" class="large-9 push-3 columns">
        $content$
    </div>

    <div class="large-3 pull-9 columns">
        <ul class="side-nav">
            <li><a href="/admin/topics">Topics</a></li>
            <li><a href="/admin/subscribers">Subscribers</a></li>
            <li><a href="/admin/subscriptions">All Subscriptions</a></li>
        </ul>
    </div>

</div>
$footer()$
<script src="/js/vendor/jquery.js"></script>
<script src="/js/foundation.min.js"></script>
<script>
    jQuery(document).foundation();
</script>
</body>
>>