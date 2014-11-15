from __future__ import unicode_literals

import sys
from os.path import abspath, dirname, join
from django.core.management import execute_from_command_line


sys.path.append(abspath(join(dirname(__file__), '..', '..')))
execute_from_command_line(
    [sys.argv[0], 'test', '--settings=case_expressions.tests.settings'] + sys.argv[1:])
