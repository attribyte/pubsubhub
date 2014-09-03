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
            <div class="panel callout radius">
                <p>The new topic was added</p>
            </div>
        </div>
    </div>
    <a class="close-reveal-modal">&#215;</a>
</div>

<div id="topic-exists-modal" class="reveal-modal tiny" data-reveal>
    <div class="row">
        <div class="large-12 columns">
            <div class="panel callout radius">
                <p>The topic already exists!</p>
            </div>
        </div>
    </div>
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
                <label><b>Change Status</b></label>
                <input type="radio" name="op" value="enable" id="esf-enable"/><label for="esf-enable">Enable</label>
                <input type="radio" name="op" value="disable" id="esf-disable"/><label for="esf-disable">Disable</label>
                <input type="radio" name="op" value="expire" id="esf-expire"/><label for="esf-expire">Expire</label>
            </div>
            <div class="small-2 columns">
                <input type="submit" class="button postfix" value="Edit"/>
            </div>
        </div>
        <input type="hidden" name="id" id="edit-subscription-form-sid"/>
    </form>
    <a class="close-reveal-modal">&#215;</a>
</div>
>>