from .column_collector import ColumnCollector



class NumericColumnCollector(ColumnCollector):
    '''Numeric column collector. Column collectors repeatidly run row collectors to extract
        rows.
    '''
    COLUMN_CHECKS = [
        *ColumnCollector.COLUMN_CHECKS,
        lambda self, ctx: self.basic_check(ctx, flag='rows_are_positive'),
    ]



class IntColumnCollector(NumericColumnCollector):
    '''Integer column collector. Column collectors repeatidly run row collectors to extract
        rows.
    '''
    pass



class FloatColumnCollector(NumericColumnCollector):
    '''Float column collector. Column collectors repeatidly run row collectors to extract rows.'''
    pass
