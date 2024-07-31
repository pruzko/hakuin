import argparse
import json
import os
import sys
import unittest



DIR_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DIR_TESTS = os.path.join(DIR_ROOT, 'tests')
DIR_TEST_CASES = os.path.join(DIR_TESTS, 'test_cases')



def discover_and_run_tests(pattern='test*.py'):
    suite = unittest.TestLoader().discover(start_dir=DIR_TEST_CASES, pattern=pattern)
    return unittest.TextTestRunner().run(suite)


if __name__ == '__main__':
    sys.path.insert(0, DIR_ROOT)

    parser = argparse.ArgumentParser(description='Run regression tests.')
    parser.add_argument('--pattern', default='test*.py', help='Pattern to select files in test/test_cases. Example: "test*.py" or "test_efficiency.py".')
    args = parser.parse_args()

    from tests import HKTest
    with open(os.path.join(DIR_TESTS, 'config.json')) as f:
        HKTest.CONFIG = json.load(f)

    result = discover_and_run_tests(pattern=args.pattern)
    if result.wasSuccessful():
        exit(0)
    else:
        exit(1)
