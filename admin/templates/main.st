main(hostname, time, content) ::= <<
$head()$
<body>
<div class="row">
    <div class="large-3 columns">
        <h1><a href="/admin"><img src="$constants.imageBase$/$constants.logo$"/></a></h1>
        <h5 class="subheader">$constants.subheader$</h5>
    </div>
    <div class="large-9 columns">
        <ul class="inline-list right">
            <li><a href="/ping">Ping</a></li>
            <li><a href="/health">Health Check</a></li>
            <li><a href="/metrics">Metrics</a></li>
            <li><a href="/metrics/notification/topic">Notification Metrics</a></li>
            <li><a href="/metrics/callback/host">Subscription Metrics</a></li>
            <li><a href="https://github.com/attribyte/pubsubhub">Source/Docs</a></li>
            <li><a href="https://pubsubhubbub.googlecode.com/git/pubsubhubbub-core-0.4.html">Spec</a></li>
        </ul>
    </div>
</div>
<div class="row">
    <div id="content" class="large-9 push-3 columns">
        $content$
    </div>

    <div class="large-3 pull-9 columns">
        <ul class="side-nav">
            <li><a href="/admin/topics/active">Topics</a></li>
            <li><a href="#" data-reveal-id="topic-add-modal">&nbsp;&nbsp;
                <small><i class="fi-plus"></i></small>
                New Topic</a></li>
            <li><a href="/admin/subscribers">Subscribers</a></li>
            <li><a href="/admin/subscriptions">Subscriptions</a></li>
            <li><a href="#" data-reveal-id="add-subscription-modal">&nbsp;&nbsp;
                <small><i class="fi-plus"></i></small>
                New Subscription</a></li>
            <li><a href="/admin/metrics">Callback Metrics</a></li>
            <li><a href="/admin/nmetrics">Notification Metrics</a></li>
            <li><a href="/admin/notifications">Recent Notifications</a></li>
            <li><a href="/admin/subscription_events">Recent Subscription Events</a></li>
            <li><a id="invalidate-caches" href="#">Invalidate Caches</a></li>
        </ul>
    </div>
</div>
$modals()$
$footer()$
<script src="$constants.jsBase$/foundation.min.js"></script>
<script>
    jQuery(document).foundation();
    jQuery(window).load(function () {
        bindTopicForm();
        bindInvalidateCaches();
    });
</script>
</body>
>>