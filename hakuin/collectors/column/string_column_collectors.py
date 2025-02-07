from .column_collector import ColumnCollector



class StringColumnCollector(ColumnCollector):
    '''String column collector. Column collectors repeatidly run row collectors to extract rows.'''
    pass



class TextColumnCollector(StringColumnCollector):
    '''Text column collector. Column collectors repeatidly run row collectors to extract rows.'''
    COLUMN_CHECKS = [
        *ColumnCollector.COLUMN_CHECKS,
        lambda self, ctx: self.basic_check(ctx, flag='rows_are_ascii'),
    ]



class BlobColumnCollector(StringColumnCollector):
    '''Blob column collector. Column collectors repeatidly run row collectors to extract rows.'''
    pass
