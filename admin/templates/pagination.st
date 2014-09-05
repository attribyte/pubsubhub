pagination() ::= <<
$if(!paging.isOnlyPage)$
<ul class="pagination">
    $if(paging.hasPrev)$
    <li class="arrow"><a href="?p=$paging.prev$">&laquo;</a></li>
    $else$
    <li class="arrow unavailable"><a href="">&laquo;</a></li>
    $endif$

    <li class="current"><a href="">$paging.curr$</a></li>

    $if(paging.hasMore)$
    <li class="arrow"><a href="?p=$paging.next$">&raquo;</a></li>
    $else$
    <li class="arrow unavailable"><a href="">&raquo;</a></li>
    $endif$
</ul>
$endif$
>>