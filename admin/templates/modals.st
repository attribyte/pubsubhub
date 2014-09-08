modals() ::= <<
<div id="topic-add-modal" class="reveal-modal tiny" data-reveal>
    <form id="topic-add-form">
        <div class="row">
            <div class="large-12 columns">
                <div class="row collapse">
                    <div class="small-2 large-2 columns">
                        <span class="prefix">Topic</span>
                    </div>
                    <div class="small-8 columns">
                        <input name="url" type="text" placeholder="URL/URI">
                    </div>
                    <div class="small-2 columns">
                        <a href="#" id="topic-add" class="button postfix">Add</a>
                    </div>
                </div>
            </div>
        </div>
    </form>
    <a class="close-reveal-modal">&#215;</a>
</div>

<div id="topic-added-modal" class="reveal-modal tiny" data-reveal>
    <div class="row">
        <div class="large-12 columns">
            <div data-alert class="alert-box info radius">
                This topic was added.
            </div>
        </div>
    </div>
    <a class="close-reveal-modal">&#215;</a>
</div>

<div id="topic-exists-modal" class="reveal-modal tiny" data-reveal>
    <div class="row">
        <div class="large-12 columns">
            <div data-alert class="alert-box warning radius">
                This topic already exists!
            </div>
        </div>
    </div>
    <a class="close-reveal-modal">&#215;</a>
</div>

<div id="add-subscription-modal" class="reveal-modal tiny" data-reveal>
    <form id="add-subscription-form">
        <div class="row">
            <div class="large-12 columns">
                <div class="row collapse">
                    <div class="small-2 large-2 columns">
                        <span class="prefix">Topic</span>
                    </div>
                    <div class="small-10 columns">
                        <input name="topicURL" type="text" placeholder="URL/URI">
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="large-12 columns">
                <div class="row collapse">
                    <div class="small-2 large-2 columns">
                        <span class="prefix">Callback</span>
                    </div>
                    <div class="small-10 columns">
                        <input name="callbackURL" type="text" placeholder="URL">
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="large-12 columns">
                <div class="row collapse">
                    <div class="small-2 large-2 columns">
                        <span class="prefix">Username</span>
                    </div>
                    <div class="small-4 columns">
                        <input name="callbackUsername" type="text" placeholder="Username">
                    </div>
                    <div class="small-2 large-2 columns">
                        <span class="prefix">Password</span>
                    </div>
                    <div class="small-4 columns">
                        <input name="callbackPassword" type="password" placeholder="Top Secret">
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="small-8 columns">
                <label><b>Initial Status</b></label>
                <input type="radio" checked="true" name="status" value="active" id="nsf-enable"/><label
                    for="nsf-enable">Enabled</label>
                <input type="radio" name="status" value="removed" id="nsf-disable"/><label
                    for="nsf-disable">Disabled</label>
                <input type="radio" name="status" value="expired" id="nsf-expire"/><label
                    for="nsf-expire">Expired</label>
                <label><b>Expiration</b>
                    <select name="extendLease">
                        <option value="hour">One Hour</option>
                        <option selected="selected" value="day">One Day</option>
                        <option value="week">One Week</option>
                        <option value="month">One Month</option>
                        <option value="never">Never</option>
                    </select>
                </label>
            </div>
            <div class="small-2 columns">
                <input type="submit" class="button postfix" value="Add"/>
            </div>
        </div>
        <input type="hidden" name="callbackAuthScheme" value="Basic"/>
    </form>
    <a class="close-reveal-modal">&#215;</a>
</div>

<div id="edit-subscription-modal" class="reveal-modal tiny" data-reveal>
    <form id="edit-subscription-form">
        <div class="row">
            <div class="small-12 colunms">
                <h6 class="subheader" id="edit-subscription-form-title"></h6>
            </div>
        </div>
        <div class="row collapse">
            <div class="small-8 columns">
                <label><b>New Status</b></label>
                <input type="radio" name="op" value="enable" id="esf-enable"/><label for="esf-enable">Enable</label>
                <input type="radio" name="op" value="disable" id="esf-disable"/><label for="esf-disable">Disable</label>
                <input type="radio" name="op" value="expire" id="esf-expire"/><label for="esf-expire">Expire</label>
                <input type="radio" name="op" value="extend" id="esf-extend"/><label for="esf-extend">Extend</label>
                <label><b>Expiration</b>
                    <select name="extendLease">
                        <option value="hour">One Hour</option>
                        <option selected="selected" value="day">One Day</option>
                        <option value="week">One Week</option>
                        <option value="month">One Month</option>
                        <option value="never">Never</option>
                    </select>
                </label>
            </div>
            <div class="small-2 columns">
                <input type="submit" class="button postfix" value="Edit"/>
            </div>
        </div>

        <div class="row collapse">
            <div class="large-12 columns">
                <label><b>Endpoint Auth</b></label>
                <div class="row collapse">
                    <div class="small-2 large-2 columns">
                        <span class="prefix">Username</span>
                    </div>
                    <div class="small-4 columns">
                        <input name="callbackUsername" type="text" placeholder="Username">
                    </div>
                    <div class="small-2 large-2 columns">
                        <span class="prefix">Password</span>
                    </div>
                    <div class="small-4 columns">
                        <input name="callbackPassword" type="password" placeholder="Top Secret">
                    </div>
                </div>
            </div>
        </div>

        <input type="hidden" name="id" id="edit-subscription-form-sid"/>
    </form>
    <a class="close-reveal-modal">&#215;</a>
</div>

<div id="subscription-exists-modal" class="reveal-modal tiny" data-reveal>
    <div class="row">
        <div class="large-12 columns">
            <div data-alert class="alert-box warning radius">
                A subscription with this topic and callback combination
                exists!
            </div>
        </div>
    </div>
    <a class="close-reveal-modal">&#215;</a>
</div>

>>