#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011-2012 F-Secure Corporation
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
import copy
import logging
import random
import time
import traceback
import os

from vim25 import ManagedObject, InvalidParameterError, TimeoutError, TaskFailedError

class VmOperations(object):
    """
    This is a collection of common VM operations that work as generators to allow
    running them in parallel
    """
    def __init__(self, vim):
        self.log = logging.getLogger('pyvsphere.vmops')
        self.log.setLevel(logging.DEBUG)
        self.vim = vim
        self._base_vm_cache = {}
        self._cluster_datastore_cache = {}

    def _get_base_vm(self, instance):
        """
        Get a VM object for the base image for cloning with a bit of caching

        @param base_vm_name: name of the VM to find

        @returns: VM object or None if not found
        """
        base_vm_name = instance['base_vm_name']
        datastore_filter = instance.get('datastore_filter', '')
        cluster = instance.get('cluster', None)
        base_vm = self._base_vm_cache.get(base_vm_name, None)
        if not base_vm:
            base_vm = self.vim.find_vm_by_name(base_vm_name, ['storage', 'summary'])
            if base_vm:
                base_vm.size = sum([x.committed for x in base_vm.storage.perDatastoreUsage])
                assert base_vm.size > 0, 'base vm size is zero? Very unlikely...'
                if cluster:
                    datastores = self._datastores_in_cluster(cluster)
                else:
                    datastores = self.vim.find_entities_by_type('Datastore', ['name', 'summary', 'info'])
                # List all available datastores that contain <datastore_filter> as substring
                base_vm.available_datastores = [x for x in datastores if datastore_filter in x.name]
                self.log.debug('Datastores for VM %s: %s' % (base_vm_name, ','.join([x.name for x in base_vm.available_datastores])))
                self._base_vm_cache[base_vm_name] = base_vm
        return base_vm

    def _datastores_in_cluster(self, clustername):
        """ Find and return the list of available datastores for a ClusterComputeResource """
        if clustername not in self._cluster_datastore_cache:
            ccr = self.vim.find_entity_by_name('ClusterComputeResource', clustername, ['name', 'datastore'])
            if not ccr:
                raise InvalidParameterError('specified ClusterComputeResource %r not found' % clustername)
            datastores = [ManagedObject(x, self.vim, ['name', 'summary', 'info']) for x in ccr.datastore]
            self._cluster_datastore_cache[clustername] = datastores
        return self._cluster_datastore_cache.get(clustername, [])

    def clone_vm(self, instance, nuke_old=False):
        """
        Perform a full clone-poweron-snapshot cycle on the instance

        This is a generator function which is used in a co-operative
        multitasking manner. Typically this would be used through run_on_instances().

        @param instance: dict of the VM instance to create
        @param nuke_old: should an existing VM with the same be nuked

        @return: generator function
        """
        def done(task):
            return (hasattr(task, 'info') and
                    (task.info.state == 'success' or
                     task.info.state == 'error'))

        def got_ip(task):
            return (hasattr(task, 'summary') and
                    getattr(task.summary.guest, 'ipAddress', None))

        def guest_tool_running(task):
            return (hasattr(task, 'summary') and
                    getattr(task.summary.guest, 'toolsRunningStatus', None) == 'guestToolsRunning')

        def place_vm(base_vm, placement_strategy='random'):
            """ Place the VM to the available datastores either randomly or wherever there is most space """
            assert placement_strategy in ['random', 'most-space'], 'unknown placement strategy, must be either \'random\' or \'most-space\''
            # Make a list of datastores that have enough space and sort it by free space
            possible_targets = sorted([x for x in base_vm.available_datastores if x.summary.freeSpace > base_vm.size], key=lambda x: x.summary.freeSpace, reverse=True)
            if len(possible_targets) == 0:
                raise InvalidParameterError('no suitable datastore found. Are they all low on space?')
            if placement_strategy == 'random':
                target = random.choice(possible_targets)
            if placement_strategy == 'most-space':
                target = possible_targets[0]
            target.summary.freeSpace -= base_vm.size
            return target

        vm_name = instance['vm_name']
        base_vm = self._get_base_vm(instance)
        if not base_vm:
            raise InvalidParameterError('base VM %s not found, check the cloud.base_vm_name property for %s' % (instance['base_vm_name'], vm_name))

        if nuke_old:
            clone = self.vim.find_vm_by_name(vm_name, ['summary'])
            if clone:
                if clone.power_state() == 'poweredOn':
                    self.log.debug('CLONE(%s) POWEROFF STARTING' % vm_name)
                    task = clone.power_off_task()
                    while not done(task):
                        task = (yield task)
                    self.log.debug('CLONE(%s) POWEOFF DONE' % vm_name)
                self.log.debug('CLONE(%s) DELETE STARTING' % vm_name)
                task = clone.delete_vm_task()
                while not done(task):
                    task = (yield task)
                self.log.debug('CLONE(%s) DELETE DONE' % vm_name)

        # Use the specified target datastore or pick one automagically based on the placement strategy
        datastore=instance.get('datastore', None)
        if not datastore:
            placement_strategy = instance.get('placement', 'random')
            datastore=place_vm(base_vm, placement_strategy=placement_strategy)

        cluster = instance.get('cluster', None)

        self.log.debug('CLONE(%s) CLONE STARTING' % vm_name)
        task = base_vm.clone_vm_task(vm_name, linked_clone=False, datastore=datastore, resource_pool=instance.get('resource_pool', None), folder=instance.get('folder'), cluster=cluster)
        while not done(task):
            task = (yield task)
        if task.info.state != 'success':
            raise TaskFailedError('CLONE(%s) failed with error: %r Details: %r' % (vm_name, task.info.error.localizedMessage, task.info.error.fault))
        self.log.debug('CLONE(%s) CLONE DONE' % vm_name)

        clone = self.vim.find_vm_by_name(vm_name)
        assert clone, 'Could not find vm %s after cloning. Must not happen. Ever.' % (vm_name)

        # Reconfigure the VM hardware as specified
        hardware = instance.get('hardware', None)
        if hardware:
            # Find if any new disks or NICs need to be added to the VM
            disks = [hardware.get('disk%d' % x) for x in xrange(10) if hardware.get('disk%d' % x)]
            nics = [hardware.get('nic%d' % x) for x in xrange(10) if hardware.get('nic%d' % x)]
            spec = self.vim.create_object('VirtualMachineConfigSpec')
            if hardware.get('ram', None):
                spec.memoryMB = int(hardware['ram'])
            if hardware.get('cpus', None):
                spec.numCPUs = int(hardware['cpus'])
            for disk in disks:
                provisioning = disk.get('provisioning', 'thin')
                assert provisioning in ['thin', 'thick'], 'disk provisioning must be on of %r, not %r' % (['thin', 'thick'], provisioning)
                disk_mode = disk.get('mode', 'persistent')
                disk_spec = clone.spec_new_disk(size=int(disk['size']), thin=provisioning=='thin', disk_mode=disk_mode)
                spec.deviceChange.append(disk_spec)
            for nic in nics:
                network = nic.get('network')
                assert network, 'network name must be specified for NICs'
                nic_type = nic.get('nic_type', 'vmxnet3')
                nic_spec = clone.spec_new_nic(network=network, nic_type=nic_type)
                spec.deviceChange.append(nic_spec)

            self.log.debug('CLONE(%s) RECONFIG_VM STARTING' % vm_name)
            task = clone.reconfig_vm_task(spec=spec)
            while not done(task):
                task = (yield task)
            self.log.debug('CLONE(%s) RECONFIG_VM DONE' % vm_name)

        self.log.debug('CLONE(%s) POWERON STARTING' % vm_name)
        task = clone.power_on_task()
        while not done(task):
            task = (yield task)
        clone.update_local_view(['summary'])
        if clone.power_state() != 'poweredOn':
            raise TaskFailedError('%s was not successfully powered on' % vm_name)
        self.log.debug('CLONE(%s) POWERON DONE' % vm_name)

        # Static IPV4 addresses can be specified for the interfaces in the VM.
        # The configuration dictionary has the following structure:
        #
        # {'eth0': {'address': '192.168.2.2',
        #           'netmask': '255.255.255.0'},
        #  'gateway': '192.168.2.1',
        #  'username': 'root',
        #  'password': '<password>'}
        #
        network_config = instance.get('network')
        if network_config:
            self.log.debug('CLONE(%s) SETTING UP IP INTERFACES' % (vm_name))
            username = instance.get('username')
            password = instance.get('password')
            if not username or not password:
                raise InvalidParameterError("'cloud.username' and 'cloud.password' need to be specified for network interface setup")
            script = ''
            for interface,parameters in [(k,v) for k,v in network_config.iteritems() if k not in ['gateway', 'username', 'password']]:
                address = parameters.get('address')
                netmask = parameters.get('netmask')
                if not address or not netmask:
                    raise InvalidParameterError("'address' and 'netmask' need to be specified for network interface configurations")
                script += 'ifconfig %s %s netmask %s' % (interface, address, netmask)
            gateway = network_config.get('gateway')
            if gateway:
                script += 'route add default gw %s' % gateway
            self.log.debug('CLONE(%s) WAITING FOR GUEST TOOLS TO START' % (vm_name))
            task = clone
            tool_wait_started = time.time()
            while not guest_tool_running(task):
                task = (yield task)
                if time.time() - tool_wait_started > 60.0:
                    raise TimeoutError('guest tools have not started in 60 seconds')
            self.log.debug('CLONE(%s) RUNNING INTERFACE SETUP SCRIPT IN VM' % (vm_name))
            clone.run_script_in_guest(script, username, password)

        self.log.debug('CLONE(%s) WAITING FOR IP' % (vm_name))
        task = clone
        while not got_ip(task):
            task = (yield task)
        self.log.debug('CLONE(%s) GOT IP: %s' % (vm_name, task.summary.guest.ipAddress))
        instance['ipv4'] = task.summary.guest.ipAddress

        self.log.debug('CLONE(%s) SNAPSHOT STARTING' % vm_name)
        task = clone.create_snapshot_task('pristine', memory=True)
        while not done(task):
            task = (yield task)
        self.log.debug('CLONE(%s) SNAPSHOT DONE' % vm_name)

    def create_snapshot(self, instance, name=None, description=None, memory=False):
        def done(task):
            return (hasattr(task, 'info') and
                    (task.info.state == 'success' or
                     task.info.state == 'error'))

        vm_name = instance['vm_name']
        vm = instance['vm']
        if not vm:
            vm = self.vim.find_vm_by_name(vm_name, ['snapshot'])
        if not vm:
            raise InvalidParameterError('VM %s not found in vSphere, something is terribly wrong here' % vm_name)

        if not os.environ.get("CLONE_WITHOUT_SNAPSHOT"):
            self.log.debug('CREATE-SNAPSHOT(%s) STARTING' % vm_name)
            task = vm.create_snapshot_task(name, description, memory)
            while not done(task):
                task = (yield task)
            self.log.debug('CREATE-SNAPSHOT(%s) DONE' % vm_name)

    def revert_to_snapshot(self, instance, name=None, wait_for_ip=True):
        """
        Perform a quick snapshot revert on a VM instance

        This is a generator function which is used in a co-operative
        multitasking manner. Typically this would be used through run_on_instances().

        @param instance: dict of the VM instance to create
        @param name: name of snapshot, revert to current snapshot if None

        @return: generator function
        """
        def done(task):
            return (hasattr(task, 'info') and
                    (task.info.state == 'success' or
                     task.info.state == 'error'))

        def got_ip(task):
            return (hasattr(task, 'summary') and
                    getattr(task.summary.guest, 'ipAddress', None))

        vm_name = instance['vm_name']
        vm = instance['vm']
        if not vm:
            vm = self.vim.find_vm_by_name(vm_name)
        if not vm:
            raise InvalidParameterError('VM %s not found in vSphere, something is terribly wrong here' % vm_name)

        self.log.debug('REVERT(%s) STARTING' % vm_name)
        if name:
            snapshots = vm.find_snapshots_by_name(name)
            if len(snapshots) != 1:
                raise InvalidParameterError('there must be one, and only one, snapshot with the name %r' % name)
            task = snapshots[0].snapshot.revert_to_snapshot_task()
        else:
            task = vm.revert_to_current_snapshot_task()
        while not done(task):
            task = (yield task)
        self.log.debug('REVERT(%s) DONE' % vm_name)

        if wait_for_ip:
            self.log.debug('REVERT(%s) WAITING FOR IP' % (vm_name))
            task = vm
            while not got_ip(task):
                task = (yield task)
            self.log.debug('REVERT(%s) GOT IP: %s' % (vm_name, task.summary.guest.ipAddress))
            instance['ipv4'] = task.summary.guest.ipAddress

    def remove_snapshot(self, instance, name=None):
        def done(task):
            return (hasattr(task, 'info') and
                    (task.info.state == 'success' or
                     task.info.state == 'error'))

        vm_name = instance['vm_name']
        vm = instance['vm']
        if not vm:
            vm = self.vim.find_vm_by_name(vm_name, ['snapshot'])
        if not vm:
            raise InvalidParameterError('VM %s not found in vSphere, something is terribly wrong here' % vm_name)

        self.log.debug('REMOVE-SNAPSHOT(%s) STARTING' % vm_name)
        snapshots = vm.find_snapshots_by_name(name)
        if len(snapshots) != 1:
            raise InvalidParameterError('there must be one, and only one, snapshot with the name %r' % name)
        task = snapshots[0].snapshot.remove_snapshot_task(remove_children=True)
        while not done(task):
            task = (yield task)
        self.log.debug('REMOVE-SNAPSHOT(%s) DONE' % vm_name)

    def power_on_off_vm(self, instance, off=False):
        """Power on/off a VM

        This is a generator function which is used in a co-operative
        multitasking manner. Typically this would be used through run_on_instances().

        @param instance: dict of the VM instance to power on/off

        @return: generator function
        """
        def done(task):
            return (hasattr(task, 'info') and
                    (task.info.state == 'success' or
                     task.info.state == 'error'))

        vm_name = instance['vm_name']
        vm = instance['vm']
        if not vm:
            vm = self.vim.find_vm_by_name(vm_name, ['summary'])
        if not vm:
            raise InvalidParameterError('VM %s not found in vSphere, something is terribly wrong here' % vm_name)

        if vm.power_state() == 'poweredOff' and not off:
            self.log.debug('POWERON(%s) STARTING', vm_name)
            task = vm.power_on_task()
            while not done(task):
                task = (yield task)
            vm.update_local_view(['summary'])
            if vm.power_state() != 'poweredOn':
                raise TaskFailedError('%s was not successfully powered on', vm_name)
            self.log.debug('POWERON(%s) DONE', vm_name)
        elif vm.power_state() == 'poweredOn' and off:
            self.log.debug('POWEROFF(%s) STARTING', vm_name)
            task = vm.power_off_task()
            while not done(task):
                task = (yield task)
            vm.update_local_view(['summary'])
            if vm.power_state() != 'poweredOff':
                raise TaskFailedError('%s was not successfully powered off', vm_name)
            self.log.debug('POWEROFF(%s) DONE', vm_name)
        else:
            self.log.debug('VM %s is already %s', vm_name, 'off' if off else 'on')

    def delete_vm(self, instance):
        """
        Power off and delete a VM

        This is a generator function which is used in a co-operative
        multitasking manner. Typically this would be used through run_on_instances().

        @param instance: dict of the VM instance to delete

        @return: generator function
        """
        def done(task):
            return (hasattr(task, 'info') and
                    (task.info.state == 'success' or
                     task.info.state == 'error'))

        vm_name = instance['vm_name']
        vm = instance['vm']
        if not vm:
            vm = self.vim.find_vm_by_name(vm_name, ['summary'])
        if not vm:
            raise InvalidParameterError('VM %s not found in vSphere, something is terribly wrong here' % vm_name)

        if vm.power_state() == 'poweredOn':
            self.log.debug('DELETE(%s) POWEROFF STARTING', vm_name)
            task = vm.power_off_task()
            while not done(task):
                task = (yield task)
            vm.update_local_view(['summary'])
            if vm.power_state() != 'poweredOff':
                raise TaskFailedError('%s was not successfully powered off', vm_name)
            self.log.debug('DELETE(%s) POWEROFF DONE', vm_name)

        self.log.debug('DELETE(%s) DELETE STARTING' % vm_name)
        task = vm.delete_vm_task()
        while not done(task):
            task = (yield task)
        self.log.debug('DELETE(%s) DELETE DONE' % vm_name)

    def update_vm(self, instance):
        """
        Get updated info from the VM instance

        This is a generator function which is used in a co-operative
        multitasking manner. Typically this would be used through run_on_instances().

        @param instance: dict of the VM instance to update

        @return: generator function
        """
        def done(task):
            return (hasattr(task, 'summary') and
                    getattr(task.summary.guest, 'ipAddress', None))

        vm_name = instance['vm_name']
        vm = instance.get('vm')
        if not vm:
            vm = self.vim.find_vm_by_name(vm_name)
        if not vm:
            raise InvalidParameterError('VM %s not found in vSphere, something is terribly wrong here' % vm_name)

        self.log.debug("UPDATE-VM(%s) WAITING FOR IP" % (vm_name))
        task = vm
        while not done(task):
            task = (yield task)
        self.log.debug("UPDATE-VM(%s) GOT IP: %s" % (vm_name, task.summary.guest.ipAddress))
        instance['ipv4'] = task.summary.guest.ipAddress

    def run_on_instances(self, instances, operation, args=None):
        """
        Run the specified operations in parallel on all the instances

        @param instances: a dict of instance_id -> instance_dict pairs
        @param operation: function to run on each instance
        @param args: dict of named arguments to pass to 'operation'

        @note: sets an 'error' key in the instance with the traceback
               in case of errors
        """
        if not args:
            args = {}
        ops = {}
        tasks = {}
        updated_instances = dict()
        for instance_id,instance_dict in instances.iteritems():
            instance_copy = copy.copy(instance_dict)
            updated_instances[instance_id] = instance_copy
            ops[instance_id] = operation(instance_copy, **args)
            tasks[instance_id] = None
        next_report = time.time() + 10.0
        while ops:
            if any(tasks.itervalues()):
                _,tasks = self.vim.update_many_objects(tasks)
            for instance_id in list(ops):
                try:
                    tasks[instance_id] = ops[instance_id].send(tasks[instance_id])
                except StopIteration:
                    del tasks[instance_id]
                    del ops[instance_id]
                except KeyboardInterrupt:
                    raise
                except Exception, err:
                    self.log.error('%s failed', instance_id)
                    updated_instances[instance_id]['error'] = traceback.format_exc()
                    del tasks[instance_id]
                    del ops[instance_id]
            if time.time() >= next_report:
                self.log.debug('%d instances still waiting', len(ops))
                next_report = time.time() + 10.0
            time.sleep(2)
        return updated_instances
