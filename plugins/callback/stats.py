# Copyright (c) 2024 StackHPC Ltd.
#
# GNU General Public License v3.0+ (see COPYING or
# https://www.gnu.org/licenses/gpl-3.0.txt)

DOCUMENTATION = '''
    name: stats
    type: notification
    short_description: Save Ansible statistics to a JSON file
    options:
        kolla_stats_path:
            description: path of the JSON statistics file.
            ini:
                - section: callback_kolla_stats
                  key: kolla_stats_path
            env:
                - name: ANSIBLE_KOLLA_STATS_PATH
            default: "~/.ansible/kolla_stats/kolla_stats.json"
            type: path
    description:
        - This plugin produces a JSON dump of statistics in a file.
        - "Statistics collected include:"
        - the count and names of failed and unreachable hosts
        - whether execution failed with no hosts remaining
'''

EXAMPLES = '''
Example statistics file contents: |
    {
        "num_failures": 1,
        "num_unreachable": 0,
        "failures": [
            "example-host-1"
        ],
        "unreachable": [],
        "no_hosts_remaining": false
    }
'''

import json
import os
from typing import List

from ansible.module_utils._text import to_bytes
from ansible.module_utils._text import to_text
from ansible.plugins.callback import CallbackBase
from ansible.utils.path import makedirs_safe


class Stats(object):
    """Kolla Ansible statistics."""

    num_failures: int
    num_unreachable: int
    failures: List[str]
    unreachable: List[str]
    no_hosts_remaining: bool

    def __init__(self, num_failures=0, num_unreachable=0, failures=None,
                 unreachable=None, no_hosts_remaining=False):
        self.num_failures = num_failures
        self.num_unreachable = num_unreachable
        self.failures = failures or []
        self.unreachable = unreachable or []
        self.no_hosts_remaining = no_hosts_remaining

    def to_json(self):
        return json.dumps(self.__dict__)


class CallbackModule(CallbackBase):
    """Callback plugin that collects Ansible statistics"""

    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'openstack.kolla.stats'
    CALLBACK_NEEDS_ENABLED = True

    def set_options(self, task_keys=None, var_options=None, direct=None):
        '''Override to set self.path '''

        super(CallbackModule, self).set_options(task_keys=task_keys,
                                                var_options=var_options,
                                                direct=direct)

        self.path = self.get_option('kolla_stats_path')
        self.no_hosts_remaining = False

    def write_stats(self, buf):
        '''Write statistics to file.'''

        buf = to_bytes(buf)
        directory = os.path.dirname(self.path)
        try:
            makedirs_safe(directory)
        except (OSError, IOError) as e:
            self._display.error(u"Unable to access or create the configured "
                                "directory (%s): %s" %
                                (to_text(directory), to_text(e)))
            raise

        try:
            path = to_bytes(self.path)
            with open(path, 'wb+') as fd:
                fd.write(buf)
        except (OSError, IOError) as e:
            self._display.error(u"Unable to write to stats file %s: %s" %
                                (to_text(self.path), to_text(e)))
            raise

    def v2_playbook_on_no_hosts_remaining(self):
        # Catch the case when no hosts remain. This means that a play has ended
        # early.
        # Note that this callback may not always fire when it should due to
        # https://github.com/ansible/ansible/issues/81549.
        self.no_hosts_remaining = True

    def v2_playbook_on_stats(self, stats):
        s = Stats(no_hosts_remaining=self.no_hosts_remaining)
        hosts = sorted(stats.processed.keys())
        for h in hosts:
            t = stats.summarize(h)
            if t['failures']:
                s.num_failures += 1
                s.failures.append(h)
            if t['unreachable']:
                s.num_unreachable += 1
                s.unreachable.append(h)
        self.write_stats(s.to_json())
