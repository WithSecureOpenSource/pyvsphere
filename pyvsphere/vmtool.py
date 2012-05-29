#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Simple demonstration script for VIM bulk operations
# Wants to be a useful tool when it grows up.
#
# Copyright 2011 F-Secure Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
import os
import optparse
import time

from vim25 import Vim, ManagedObject
from vmops import VmOperations


class VmTool(object):
    def __init__(self, vi_url, vi_username, vi_password, vi_version, debug=False):
        self.debug = debug
        self.log = logging.getLogger('pyvsphere.vmtool')
        if self.debug:
            self.log.setLevel(logging.DEBUG)
        self.vi_url = vi_url or os.environ.get('VI_URL')
        assert self.vi_url, 'either the enviroment variable VI_URL or the url parameter needs to be specified'
        self.vi_username = vi_username or os.environ.get('VI_USERNAME')
        assert self.vi_username, 'either the enviroment variable VI_USERNAME or the username parameter needs to be specified'
        self.vi_password = vi_password or os.environ.get('VI_PASSWORD')
        assert self.vi_password, 'either the enviroment variable VI_PASSWORD or the password parameter needs to be specified'
        self.vi_version = vi_version or os.environ.get('VI_VERSION')

        self.vim = Vim(self.vi_url, debug=False, version=self.vi_version)
        self.log.debug('CONNECTION complete')
        self.vim.login(self.vi_username, self.vi_password)
        self.log.debug('LOGIN complete')

        self.vmops = VmOperations(self.vim)

    def test(self, options):
        """ Placeholder for random hacking so --test has something to run """
        print 'Status: 95% complete ...'
        time.sleep(10.0)

    def vm_names_from_options(self, options):
        if options.count == 1:
            yield options.vm_name
        else:
            for i in range(options.count):
                 yield '%s-%02d' % (options.vm_name, i)

    def clone_vms(self, options):
        ops = {}
        tasks = {}
        for vm_name in self.vm_names_from_options(options):
            instance = dict(vm_name=vm_name,
                            base_vm_name=options.base_image,
                            datastore_filter=options.datastore_filter,
                            folder=options.folder,
                            resource_pool=options.resource_pool)
            ops[vm_name] = self.vmops.clone_vm(instance, nuke_old=True)
            tasks[vm_name] = None

        return self._run_operations(ops, tasks)

    def _run_operations(self, ops, tasks):
        next_report = time.time() + 10.0
        while ops:
            if any(tasks.itervalues()):
                _,tasks = self.vim.update_many_objects(tasks)
            for op_key in list(ops):
                try:
                    tasks[op_key] = ops[op_key].send(tasks[op_key])
                except StopIteration:
                    del tasks[op_key]
                    del ops[op_key]
                except Exception, err:
                    self.log.exception('%s failed', op_key)
                    del tasks[op_key]
                    del ops[op_key]
            if time.time() >= next_report:
                self.log.debug('%d instances still waiting', len(ops))
                next_report = time.time() + 10.0

            time.sleep(2)

    def delete_vms(self, options):
        """ Delete a batch of VMs """
        instances = [dict(vm_name=x, vm=None) for x in self.vm_names_from_options(options)]
        ops = dict((instance['vm_name'],self.vmops.delete_vm(instance)) for instance in instances)
        tasks = dict((instance['vm_name'],None) for instance in instances)
        return self._run_operations(ops, tasks)

    def list_ips(self, options):
        """ List the IP addresses of a number of VMs """
        clones = [self.vim.find_vm_by_name(x) for x in self.vm_names_from_options(options)]
        clones = [x for x in clones if x]
        # Update all the clones once
        _ = [x.update_local_view(['name', 'summary']) for x in clones]

        waiting_for_ips = True
        while waiting_for_ips:
            self.log.debug('-' * 40)
            have_it_all = True

            # Update the empty ones
            for clone in [clone for clone in clones if not getattr(clone.summary.guest, 'ipAddress', None)]:
                clone.update_local_view(['name', 'summary'])

            have_it_all = True
            for clone in clones:
                ip_address = getattr(clone.summary.guest, 'ipAddress', None)
                if not ip_address:
                    have_it_all = False
                if ip_address:
                    self.log.debug('%s: %s', clone.name, ip_address)
                else:
                    self.log.debug('%s: %s', ip_address, '<NO IP ASSIGNED YET>')
            if have_it_all:
                break
        for clone in clones:
            print '%s: %s' % (clone.name, clone.summary.guest.ipAddress)

    def snapshot(self, options):
        vm = self.vim.find_vm_by_name(options.vm_name)
        vm.create_snapshot(options.snapshot, memory=True)

    def revert(self, options):
        vm = self.vim.find_vm_by_name(options.vm_name)
        vm.revert_to_current_snapshot()


def main():
    parser = optparse.OptionParser('Usage: %prog [options]')
    parser.add_option('--debug',
                      action='store_true', dest='debug', default=False,
                      help='Turn on noisy logging')
    parser.add_option('--clone',
                      action='store_true', dest='clone', default=False,
                      help='Clone VMs from a base image')
    parser.add_option('--snapshot',
                      dest='snapshot', default=None,
                      help='Take a snapshot with <name>')
    parser.add_option('--revert',
                      action='store_true', dest='revert', default=False,
                      help='Revert to current snapshot')
    parser.add_option('--delete',
                      action='store_true', dest='delete', default=False,
                      help='Delete VMs')
    parser.add_option('--list-ips',
                      action='store_true', dest='list_ips', default=False,
                      help='List IP addresses of VMs')
    parser.add_option('--test',
                      action='store_true', dest='test', default=False,
                      help='do some testing craziness')
    parser.add_option('--count', dest='count', type='int', default=1,
                      help='Number of VMs to process')
    parser.add_option('--base-image', dest='base_image',
                      help='Name of the image to use as base for cloning')
    parser.add_option('--datastore-filter', dest='datastore_filter', default='',
                      help='place the clones VMs to datastores which contain the filter substring')
    parser.add_option('--vm-name', dest='vm_name',
                      help='Name of VM (used as a prefix in batch operations)')
    parser.add_option('--folder', dest='folder', default='',
                      help='destination folder for the clones, in the format of Data Center/vm/Any/Folder/Name')
    parser.add_option('--resource-pool', dest='resource_pool', default='',
                      help='resource pool for the clones. Defaults to the root pool if not specified.')
    parser.add_option('--username', dest='vi_username', default=None,
                      help='vSphere user name')
    parser.add_option('--password', dest='vi_password', default=None,
                      help='vSphere password')
    parser.add_option('--url', dest='vi_url', default=None,
                      help='vSphere URL (https://<your_server>/sdk)')
    parser.add_option('--vsphere-version', dest='vi_version', default=None,
                      help='vSphere version number)')
    parser.add_option('-v', '--verbose',
                      action='store_true', dest='verbose', default=False,
                      help='keeps you well informed when running')
    (options, args) = parser.parse_args()

    vmtool = VmTool(options.vi_url, options.vi_username, options.vi_password, options.vi_version, options.verbose)
    
    if options.clone:
        vmtool.clone_vms(options)

    if options.list_ips:
        vmtool.list_ips(options)

    if options.delete:
        vmtool.delete_vms(options)

    if options.snapshot:
        vmtool.snapshot(options)

    if options.revert:
        vmtool.revert(options)

    if options.test:
        vmtool.test(options)

if __name__ == '__main__':
    main()
