# Experiment 1: evaluate performence of Hakuin on DB schemas
import code
import json
import string
import evaluate as ev
import hakuin



def evaluate(data, **kwargs):
    total = ev.count_data(data)
    res_bin = ev.count_bin_search(data, hakuin.CHARSET_SCHEMA)
    res_bin_sqlmap = ev.count_bin_search(data, ev.CHARSET_ASCII)
    res_hakuin = ev.count_hakuin(data, **kwargs)
    
    return {
        'total': total,
        'bin_search': res_bin / total,
        'bin_search_sqlmap': res_bin_sqlmap / total,
        'hakuin': res_hakuin / total,
    }


def main():
    m_tables = hakuin.get_model_tables()
    m_columns = hakuin.get_model_columns()

    data = ev.load_dbanswers_data()
    data_tab = [t['table'] for tables in data for t in tables]
    data_col = [c for tables in data for t in tables for c in t['columns']]

    kwargs = {
        'ngram': m_tables.max_ngram,
        'mode': 'schema',
        'selection': 'huffman',
        'downgrading': True,
        'threshold_scores': None,
        'threshold_counts': None,
        'gradual_miss_resolve': False,
        'multi_word': False,
    }

    print('=== TABLES ===')
    best: 5gram, huffman, downgrading (-0.03), thresholding_scores 0.02 (-0.02)
    res = evaluate(data_tab, model=m_tables, **kwargs)
    print(json.dumps(res, indent=4))

    # print('=== COLUMNS ===')
    # best: 5gram, huffman, downgrading (-0.005), thresholding_scores 0.02 (-0.02)
    # res = evaluate(data_col, model=m_columns, **kwargs)
    # print(json.dumps(res, indent=4))

    # code.interact(local=dict(globals(), **locals()))

if __name__ == '__main__':
    main()