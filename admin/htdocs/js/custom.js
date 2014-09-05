function handleXHRError(xhr, textStatus, errorThrown) {
    alert(xhr.code);
    alert(xhr.responseText);
}

function getId(xid) {
    var index = xid.indexOf('_');
    if (index > 0 && index < xid.length - 1) {
        return xid.substring(index + 1);
    } else {
        return -1;
    }
}

function bindTopicForm() {

    $('#topic-add-form').bind('submit', function () {
        event.preventDefault();
        addTopic();
    });

    $('#topic-add').click(function (event) {
        event.preventDefault();
        addTopic();
    });

    $('#topic-added-modal').bind('closed', function () {
        document.location.href = '/admin/topics';
    });

    $('#edit-subscription-form').bind('submit', function () {
        event.preventDefault();
        editSubscription();
        return false;
    });

    $('#add-subscription-form').bind('submit', function () {
        event.preventDefault();
        addSubscription();
    });
}

function bindSubscriptionEdit() {
    $('.ss').click(function (event) {
        var id = getId(event.target.id);
        var currStatus = $('#ss_' + id).attr('name');
        var href = $('#ss_' + id).attr('href');
        var proto = href.indexOf("://");
        if (proto > 0) {
            href = href.substring(proto + 3);
        }
        var title = href + " is currently <b>" + currStatus.toLowerCase() + "</b>";

        event.preventDefault();
        $('#edit-subscription-form-title').html(title);
        $('#edit-subscription-form-sid').val(id);
        if (currStatus == "ACTIVE") {
            $('#esf-extend').prop('checked', true);
        } else if (currStatus == "EXPIRED") {
            $('#esf-extend').prop('checked', true);
        } else {
            $('#esf-enable').prop('checked', false);
            $('#esf-expire').prop('checked', false);
            $('#esf-disable').prop('checked', false);
            $('#esf-extend').prop('checked', false);
        }
        $('#edit-subscription-modal').foundation('reveal', 'open');
    });
}

function addTopic() {
    $.ajax({
        type: 'POST',
        url: "/admin/topic",
        data: $('#topic-add-form').serialize(),
        statusCode: {
            200: function () {
                $('#topic-add-modal').foundation('reveal', 'close');
                $('#topic-exists-modal').foundation('reveal', 'open');
            },
            201: function () {
                $('#topic-add-modal').foundation('reveal', 'close');
                $('#topic-added-modal').foundation('reveal', 'open');
            }
        },
        error: handleXHRError
    });
}

function editSubscription() {
    var sid = $("#edit-subscription-form-sid").val();
    $.ajax({
        type: 'POST',
        url: "/admin/subscription/" + sid,
        data: $('#edit-subscription-form').serialize(),
        success: function (html, textStatus) {
            if (textStatus == "success") {
                location.reload();
            } else {
                alert("Error: " + textStatus);
            }
            $('#edit-subscription-modal').foundation('reveal', 'close');
        },
        error: handleXHRError
    });
}

function addSubscription() {
    $.ajax({
        type: 'POST',
        url: "/admin/subscription",
        data: $('#add-subscription-form').serialize(),
        success: function (html, textStatus) {
            var id = html.toString();
            if (id == "ok") {
                $('#add-subscription-modal').foundation('reveal', 'close');
                location.href = "/admin/subscriptions";
            } else {
                $('#subscription-exists-modal').foundation('reveal', 'open');
            }
        },
        error: handleXHRError
    });
}