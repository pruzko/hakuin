# Experiment 2: evaluate performence of Hakuin on generic data
import code
import copy
import json
import string
import evaluate as ev
import hakuin

# TODO
import random
random.seed(42)



def _evaluate(data, kwargs_generic, kwargs_adaptive):
    kwargs_generic = copy.deepcopy(kwargs_generic)
    kwargs_adaptive = copy.deepcopy(kwargs_adaptive)

    total = ev.count_data(data)
    res_bin = ev.count_bin_search(data, string.ascii_lowercase)
    res_bin_sqlmap = ev.count_bin_search(data, ev.CHARSET_ASCII)

    if kwargs_adaptive is None:
        res_hakuin = ev.count_hakuin(data, **kwargs_generic)
    else:
        batch_size = kwargs_adaptive.pop('batch_size')
        res_hakuin = ev.count_hakuin_adaptive(data, batch_size, kwargs_generic, kwargs_adaptive)
    
    return {
        'total': total,
        'bin_search': res_bin / total,
        'bin_search_sqlmap': res_bin_sqlmap / total,
        'hakuin': res_hakuin / total,
    }


def evaluate(db, kwargs_generic, kwargs_adaptive):
    kwargs_generic = copy.deepcopy(kwargs_generic)
    kwargs_adaptive = copy.deepcopy(kwargs_adaptive)

    res = {
        'average': {
            'total': 0,
            'bin_search': 0.0,
            'bin_search_sqlmap': 0.0,
            'hakuin': 0.0,
        },
        'columns': {}
    }

    for column, data in db.items():
        # kwargs_generic['multi_word'] = ' ' in ''.join(data)
        res['columns'][column] = _evaluate(data, kwargs_generic, kwargs_adaptive)

    for column, res_col in res['columns'].items():
        res['average']['total'] += res_col['total']
        res['average']['bin_search'] += res_col['bin_search'] * res_col['total']
        res['average']['bin_search_sqlmap'] += res_col['bin_search_sqlmap'] * res_col['total']
        res['average']['hakuin'] += res_col['hakuin'] * res_col['total']

    res['average']['bin_search'] /= res['average']['total']
    res['average']['bin_search_sqlmap'] /= res['average']['total']
    res['average']['hakuin'] /= res['average']['total']

    return res


def explore_th_scores(db, max_th, step, kwargs_generic):
    kwargs_generic = copy.deepcopy(kwargs_generic)

    for i in range(int(max_th / step)):
        kwargs_generic['threshold_scores'] = step * i
        res = evaluate(db, kwargs_generic, None)
        print(f'th: {kwargs_generic["threshold_scores"]}, res: {res["all"]["hakuin"]}')


def main():
    model = hakuin.get_model_generic()
    db = ev.load_generic_db()
    # TODO
    # for k, v in db.items():
    #     db[k] = v[:100]

    kwargs_generic = {
        'model': model,
        'ngram': model.max_ngram,
        'mode': 'generic',
        'selection': 'huffman',
        'downgrading': True,
        'threshold_scores': 0.11,
        'threshold_counts': None,
        'gradual_miss_resolve': False,
        'multi_word': True,
    }

    kwargs_adaptive = {
        'batch_size': 1,
        # 'batch_size': 10,
        'ngram': model.max_ngram,
        'mode': 'generic',
        'selection': 'huffman',
        'downgrading': True,
        'threshold_scores': None,
        'threshold_counts': None,
        'gradual_miss_resolve': False,
        'multi_word': False,
    }

    # res = evaluate(db, kwargs_generic=kwargs_generic, kwargs_adaptive=None)
    # print(json.dumps(res, indent=4))

    # best: 5gram, huffman, downgrading (-2.0). generic model seems to be insignificant
    db = {'users.address': db['users.address']}
    res = evaluate(db, kwargs_generic=None, kwargs_adaptive=kwargs_adaptive)
    # print(json.dumps(res['average'], indent=4))
    print(json.dumps(res, indent=4))

    # print('### TH ###')
    # explore_th_scores(db, 0.2, 0.01, kwargs_generic)

    # code.interact(local=dict(globals(), **locals()))


if __name__ == '__main__':
    main()