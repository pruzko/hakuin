import json
import os
import subprocess
import unittest

from hakuin.utils import DIR_ROOT



class HKTest(unittest.TestCase):
    CONFIG = None


    @staticmethod
    def generate_test(hk_args, result, n_requests, order_important=False):
        def hk_test(self):
            res = self.run_hk(hk_args)
            self.assertEqual(res['stats']['n_requests'], n_requests)

            res_data = res['data'] if order_important else self.sort_lists(res['data'])
            res_data_correct = result if order_important else self.sort_lists(result)
            self.assertEqual(res_data, res_data_correct)

        return hk_test


    @staticmethod
    def sort_lists(d):
        if isinstance(d, dict):
            return {k: HKTest.sort_lists(v) for k, v in d.items()}
        elif isinstance(d, list):
            return sorted(HKTest.sort_lists(x) for x in d)
        return d


    def run_hk(self, args):
        hk_args = [args.pop('url')]
        hk_args += [arg for arg_pair in args.items() for arg in arg_pair]
        hk_path = os.path.abspath(os.path.join(DIR_ROOT, '..', 'hk.py'))
        res = subprocess.run(['python', hk_path, *hk_args], capture_output=True, text=True)

        if res.returncode != 0:
            self.fail(f'hk.py failed with error code: {res.returncode} - {res.stderr}')

        try:
            res = json.loads(res.stdout)
        except json.JSONDecodeError as e:
            self.fail(f'JSON decoding failed: {e}')

        self.assertTrue(type(res) == dict, f'Malformed result: {res}')
        self.assertTrue('stats' in res, f'Malformed result: {res}')
        self.assertTrue('data' in res, f'Malformed result: {res}')
        return res