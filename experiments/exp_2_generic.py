# Experiment 2: evaluate performence of Hakuin on generic data
import code
import json
import string
import evaluate as ev
import hakuin


def evaluate(data, model, downgrading, threshold_scores, threshold_counts, gradual_miss_resolve):
    total = ev.count_data(data)
    res_bin = ev.count_bin_search(data, string.ascii_lowercase)
    res_bin_sqlmap = ev.count_bin_search(data, ev.CHARSET_ASCII)
    res_hakuin = ev.count_hakuin(data, model, mode='generic', downgrading=downgrading, threshold_scores=threshold_scores, threshold_counts=threshold_counts, gradual_miss_resolve=gradual_miss_resolve)
    
    return {
        'total': total,
        'bin_search': res_bin / total,
        'res_bin_sqlmap': res_bin_sqlmap / total,
        'hakuin': {order: cnt / total for order, cnt in res_hakuin.items()},
    }


def main():
    model = hakuin.get_model_generic()
    data = ev.load_generic_db()
    data = [x for v in data.values() for x in v]

    import random
    random.seed(42)
    data = random.sample(data, 100)

    print('=== GENERIC ===')
    # for i in range(31):
    #     th = i / 100
    #     res = evaluate(data, model, downgrading=True, threshold_scores=th)
    #     print(f'th: {th}, res: {res["hakuin"]["5_huffman"]}')

    # for th in range(0, 100, 10):
    #     res = evaluate(data, model, downgrading=True, threshold_scores=0.11, threshold_counts=th, gradual_miss_resolve=True)
    #     print(f'th: {th}, res: {res["hakuin"]["5_huffman"]}')

    res = evaluate(data, model, downgrading=True, threshold_scores=0.11, threshold_counts=None, gradual_miss_resolve=False)
    print(json.dumps(res, indent=4))

    # code.interact(local=dict(globals(), **locals()))

if __name__ == '__main__':
    main()