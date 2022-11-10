# Experiment 1: evaluate performence of Hakuin on DB schemas
import code
import json
import string
import evaluate as ev
import hakuin


def evaluate(data, model, threshold_scores):
    total = ev.count_data(data)
    res_bin = ev.count_bin_search(data, hakuin.CHARSET_SCHEMA)
    res_bin_sqlmap = ev.count_bin_search(data, ev.CHARSET_ASCII)
    res_hakuin = ev.count_hakuin(data, model, mode='schema', downgrading=True, threshold_scores=threshold_scores)
    
    return {
        'total': total,
        'bin_search': res_bin / total,
        'res_bin_sqlmap': res_bin_sqlmap / total,
        'hakuin': {order: cnt / total for order, cnt in res_hakuin.items()},
    }


def analyze_per_idx(data, hakuin, ngram, selection):
    max_len = len(max(data, key=len))
    res = {str(i): {'n': 0, 'total': 0.0} for i in range(max_len + 1)}

    for d in data:
        ev.hakuin_analyze_per_idx(hakuin, d, ngram=ngram, res=res, charset=hakuin.ALPHABET, selection=selection)

    for v in res.values():
        v['avg'] = v['total'] / v['n'] if v['n'] else 0.0

    return res


def main():
    m_tables = hakuin.get_model_tables()
    m_columns = hakuin.get_model_columns()

    data = ev.load_dbanswers_data()
    data_tab = [t['table'] for tables in data for t in tables]
    data_col = [c for tables in data for t in tables for c in t['columns']]

    print('=== TABLES ===')
    # for i in range(31):
    #     th = i / 100
    #     res = evaluate(data_tab, m_tables, threshold_scores=th)
    #     print(f'th: {th}, res: {res["hakuin"]["5_huffman"]}')

    res = evaluate(data_tab, m_tables, threshold_scores=0.05)
    print(json.dumps(res, indent=4))

    # print('=== COLUMNS ===')
    # res = evaluate(data_col, m_columns, threshold_scores=0.1)
    # print(json.dumps(res, indent=4))

    # res_p = analyze_per_idx(data_tab, m_tables, ngram=4, selection='plain')
    # res_h = analyze_per_idx(data_tab, m_tables, ngram=4, selection='huffman')
    # for idx in res_p:
    #     avg_p = round(res_p[str(idx)]["avg"], 2)
    #     avg_h = round(res_h[str(idx)]["avg"], 2)
    #     winner = ['P', 'H'][avg_h <= avg_p]
    #     print(f'{idx}] winner: {winner} P: {avg_p}\tH: {avg_h}')

    # code.interact(local=dict(globals(), **locals()))

if __name__ == '__main__':
    main()