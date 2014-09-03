main(hostname, time, content) ::= <<
$head()$
<body>
<div class="row">
    <div class="large-3 columns">
        <h1><a href="/admin"><img src="$constants.imageBase$/$constants.logo$"/></a></h1>
        <h4 class="subheader">pubsubhub</h4>
    </div>
    <div class="large-9 columns">
        <ul class="inline-list right">
            <li><span>$hostname$ @ $time; format="long"$</span></li>
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
            <li><a href="#" data-reveal-id="topic-add-modal">&nbsp; &nbsp;New Topic</a></li>
            </li>
            <li><a href="/admin/subscribers">Subscribers</a></li>
            <li><a href="/admin/subscriptions">All Subscriptions</a></li>
        </ul>
    </div>
</div>
$modals()$
$footer()$
<script src="$constants.jsBase$/foundation.min.js"></script>
<script>
    jQuery(document).foundation();
    bindTopicForm();
</script>
</body>
>>