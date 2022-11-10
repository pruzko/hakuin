import string
from hakuin.Model import Model, get_model_tables, get_model_columns, get_model_generic



CHARSET_SCHEMA = list(string.ascii_lowercase + string.digits + '_#@') + ['</s>']