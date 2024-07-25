import json
import os
import unittest

from tests import HKTest



DIR_FILE = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(DIR_FILE, '..', 'dbs', 'test_efficiency_content.json')) as f:
    CONTENT_DATA = json.load(f)



class TestPerformance(HKTest):
    def test_meta(self):
        with open(os.path.join(DIR_FILE, '..', 'dbs', 'test_efficiency_meta.json')) as f:
            correct_res = json.load(f)

        res = self.run_hk({
            'url': '',
            '-D': 'sqlite',
            '-R': os.path.abspath(os.path.join(DIR_FILE, '..', 'OfflineRequester.py:MetaOfflineRequester')),
            '-x': 'meta',
        })

        self.assertEqual(res['stats']['n_requests'], 27376)
        self.assertEqual(self.sort_lists(res['data']), self.sort_lists(correct_res))


    def test_content_users_username(self):
        res = self.run_hk({
            'url': '',
            '-D': 'sqlite',
            '-R': os.path.abspath(os.path.join(DIR_FILE, '..', 'OfflineRequester.py:ContentOfflineRequester')),
            '-t': 'users',
            '-c': 'username',
        })
        self.assertEqual(res['stats']['n_requests'], 42129)
        self.assertEqual(res['data'], [user['username'] for user in CONTENT_DATA['users']])


    def test_content_users_sex(self):
        res = self.run_hk({
            'url': '',
            '-D': 'sqlite',
            '-R': os.path.abspath(os.path.join(DIR_FILE, '..', 'OfflineRequester.py:ContentOfflineRequester')),
            '-t': 'users',
            '-c': 'sex',
        })
        self.assertEqual(res['stats']['n_requests'], 1613)
        self.assertEqual(res['data'], [user['sex'] for user in CONTENT_DATA['users']])


    def test_content_users_password(self):
        res = self.run_hk({
            'url': '',
            '-D': 'sqlite',
            '-R': os.path.abspath(os.path.join(DIR_FILE, '..', 'OfflineRequester.py:ContentOfflineRequester')),
            '-t': 'users',
            '-c': 'password',
        })
        self.assertEqual(res['stats']['n_requests'], 137120)
        self.assertEqual(res['data'], [user['password'] for user in CONTENT_DATA['users']])
