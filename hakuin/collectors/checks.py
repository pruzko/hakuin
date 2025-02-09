from hakuin.utils import snake_to_pascal_case



async def check_flag(requester, dbms, ctx, name, true_if_true=None, false_if_false=None):
    '''Checks a context flag or send a request to infer a missing flag.

    Params:
        requester (Requester): requester
        dbms (DBMS): database engine
        ctx (Context): collection context
        name (str): flag name
        true_if_true (str|None): automatically turn the flag on if this flag is on
        false_if_false (str|None): automatically turn the flag off if this flag is off
    '''
    flag = getattr(ctx, name) if hasattr(ctx, name) else None

    if true_if_true and getattr(ctx, true_if_true) is True:
        flag = True

    if false_if_false and getattr(ctx, false_if_false) is False:
        flag = False

    if flag is None:
        query_cls_name = snake_to_pascal_case(f'query_{name}')
        query_cls = dbms.query_cls_lookup(query_cls_name)
        flag = await requester.run(query=query_cls(dbms), ctx=ctx)

    return flag
