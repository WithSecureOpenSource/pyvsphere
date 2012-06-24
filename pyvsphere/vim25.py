# -*- coding: utf-8 -*-
#
# Python interface to VMware vSphere API
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
import logging
import httplib
import time
import suds
import urllib2

class TimeoutError(Exception):
    pass

class ObjectNotFoundError(Exception):
    pass

class TaskFailedError(Exception):
    pass

class InvalidParameterError(Exception):
    pass

class Vim(object):
    """
    Interface class for VMware VIM API over SOAP
    """
    def __init__(self, url, debug=False, version=None):
        """
        @param url: URL to the vSphere server (eg.: https://foosphere/sdk)
        @param debug: Run in debug mode (very noisy)
        """
        self.task_timeout = 600
        if debug:
            logging.basicConfig(level=logging.INFO)
            logging.getLogger('suds.client').setLevel(logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
            logging.getLogger('suds').setLevel(logging.INFO)

        try:
            self.soapclient = suds.client.Client(url+"/vimService.wsdl")
        except Exception, e:
            if 'imported schema (urn:reflect)' in str(e):
                assert False, 'WSDL file set incomplete on the vSphere server. See http://kb.vmware.com/kb/2010507'
            else:
                raise

        self.soapclient.set_options(location=url)
        self.soapclient.set_options(cachingpolicy=1)
        self.service_instance = ManagedObjectReference(_type='ServiceInstance',
                                                       value='ServiceInstance')
        self.service_content = self.invoke('RetrieveServiceContent',
                                           _this=self.service_instance)
        self.property_collector = self.service_content.propertyCollector
        self.full_traversal_specs = self._build_full_traversal_specs()

    def create_object(self, object_type):
        return self.soapclient.factory.create("ns0:%s" % object_type)

    def invoke(self, method, **kwargs):
        try:
            return getattr(self.soapclient.service, method)(**kwargs)
        except httplib.BadStatusLine:
            return None

    def invoke_task(self, method, **kwargs):
        """
        Execute a task and poll until it completes or times out

        @param method: name of method to invoke
        @param **kwargs: keyword arguments to be passed to the method

        @returns: True on success
        """
        task_mor = self.invoke(method=method, **kwargs)
        task = ManagedObject(mor=task_mor, vim=self)
        start_time = time.time()
        while True:
            task.update_local_view(properties=['info'])
            if task.info.state == 'success':
                return True
            elif task.info.state == 'error':
                raise TaskFailed(error=task.info.error.localizedMessage)
            time.sleep(1)
            if time.time()-start_time > self.task_timeout:
                raise TimeoutError, "task timed out after %d seconds" % self.task_timeout

    def wait_for_task(self, task):
        """
        Keep pollint until a task completes or times out

        @param task: task object to wait for

        @returns: True on success
        """
        start_time = time.time()
        while True:
            task.update_local_view(properties=['info'])
            if task.info.state == 'success':
                return True
            elif task.info.state == 'error':
                raise TaskFailed(error=task.info.error.localizedMessage)
            time.sleep(1)
            if time.time()-start_time > self.task_timeout:
                raise TimeoutError, "task timed out after %d seconds" % self.task

    def update_many_objects(self, objects):
        """
        Get an update on a list of tasks at once

        @param tasks: dict of task objects to update

        @returns: dict of updated tasks (empty ones untouched)

        @note: It handles 'Task' and 'VirtualMachine' object types
         """
        property_types = [ ('Task', ['info']),
                           ('VirtualMachine', ['name', 'summary']) ]
        prop_set = []
        for ptype,ppath in property_types:
            property_spec = self.create_object('PropertySpec')
            property_spec.type = ptype
            property_spec.all = False
            property_spec.pathSet = ppath
            prop_set.append(property_spec)
        object_set = []
        object_map = {}
        updated_objects = {}
        for key,obj in objects.iteritems():
            # Do not update empty objects but return them as supplied
            if not obj:
                updated_objects[key] = obj
                continue
            object_spec = self.create_object('ObjectSpec')
            object_spec.obj = obj.mor
            object_set.append(object_spec)
            object_map[obj.mor.value] = key
        pfs = self.create_object('PropertyFilterSpec')
        pfs.propSet = prop_set
        pfs.objectSet = object_set
        object_contents = self.invoke('RetrieveProperties',
                                          _this=self.property_collector,
                                          specSet=pfs)
        if not object_contents or len(object_contents) != len(objects):
            return False, objects
        for object_content in object_contents:
            updated_object = ManagedObject(mor=object_content.obj, vim=self)
            for prop in object_content.propSet:
                if prop.val.__class__.__name__.startswith('Array'):
                    # suds embeds Array-type data into lists
                    setattr(updated_object, prop.name, prop.val[0])
                else:
                    setattr(updated_object, prop.name, prop.val)
            updated_objects[object_map[object_content.obj.value]] = updated_object
        return True, updated_objects

    def login(self, username, password):
        """
        Log in to the vSphere service

        @param username: name of user
        @param password: password to use
        """
        self.invoke('Login', _this=self.service_content.sessionManager,
                    userName=username, password=password)

    def logout(self):
        """
        Log out from the vSphere service
        """
        self.invoke('Logout', _this=self.service_content.sessionManager)

    def find_entities_by_type(self, entity_type, properties=None):
        """
        Find vSphere entities (ManagedObjects) by type

        @param entity_type: type of the entity (for example 'DataStore')
        @param properties: list of properties to fetch immediately

        @return: list of found objects
        """
        # Prop spec
        propspec = self.create_object('PropertySpec')
        propspec.type = entity_type
        propspec.all = False
        propspec.pathSet = ['name']
        if properties:
            propspec.pathSet.extend(properties)
        # Obj spec
        objspec = self.create_object('ObjectSpec')
        objspec.obj = self.service_content.rootFolder
        objspec.selectSet = self.full_traversal_specs
        # Filter spec
        propfilterspec = self.create_object('PropertyFilterSpec')
        #propfilterspec.reportMissingObjectsInResults = None
        propfilterspec.propSet = [propspec]
        propfilterspec.objectSet = [objspec]
        result = self.invoke('RetrieveProperties',
                             _this=self.property_collector,
                             specSet=propfilterspec)
        return [self.object_from_object_content(x) for x in result]

    def object_from_object_content(self, object_content):
        if object_content.obj._type == 'VirtualMachine':
            obj = VirtualMachine(object_content.obj, self)
        else:
            obj = ManagedObject(object_content.obj, self)
        obj.update_object(object_content)
        return obj

    def find_entity_by_name(self, entity_type, entity_name, properties=None):
        """
        Find a specific vSphere entity (ManagedObjects) by its name

        @param entity_type: type of the entity (for example 'DataStore')
        @param entity_name: name of the entity
        @param properties: list of properties to fetch immediately

        @return: object or None if not found
        """
        entities = self.find_entities_by_type(entity_type, properties=properties)
        for e in entities:
            if e.name == entity_name:
                return e
        return None

    def find_vm_by_name(self, vmname, properties=None):
        """
        Find a virtual machine by its name

        @param vmname: name of VM

        @return: VirtualMachine object or None if not found
        """
        return self.find_entity_by_name('VirtualMachine', vmname, properties=properties)

    def _build_full_traversal_specs(self):
        def selection_spec(specname):
            selspec = self.create_object('SelectionSpec')
            selspec.name = specname
            return selspec
        # Description of the traversal specs needed to walk the
        # whole inventory. Yes, this is magic.
        traversals = [
            dict(name = 'rp_to_rp',
                 type = 'ResourcePool',
                 path = 'resourcePool',
                 selectSet = [selection_spec('rp_to_rp'),
                              selection_spec('rp_to_vm')]),
            dict(name = 'rp_to_vm',
                 type = 'ResourcePool',
                 path = 'vm'),
            dict(name = 'cr_to_rp',
                 type = 'ComputeResource',
                 path = 'resourcePool',
                 selectSet = [selection_spec('rp_to_rp'),
                              selection_spec('rp_to_vm')]),
            dict(name = 'cr_to_ds',
                 type = 'ComputeResource',
                 path = 'datastore'),
            dict(name = 'cr_to_h',
                 type = 'ComputeResource',
                 path = 'host'),
            dict(name = 'dc_to_hf',
                 type = 'Datacenter',
                 path = 'hostFolder',
                 selectSet = [selection_spec('f_to_f')]),
            dict(name = 'dc_to_vmf',
                 type = 'Datacenter',
                 path = 'vmFolder',
                 selectSet = [selection_spec('f_to_f')]),
            dict(name = 'dc_to_nf',
                 type = 'Datacenter',
                 path = 'networkFolder',
                 selectSet = [selection_spec('f_to_f')]),
            dict(name = 'dc_to_dsf',
                 type = 'Datacenter',
                 path = 'datastoreFolder',
                 selectSet = [selection_spec('f_to_f')]),
            dict(name = 'h_to_vm',
                 type = 'HostSystem',
                 path = 'vm',
                 selectSet = [selection_spec('f_to_f')]),
            dict(name = 'f_to_f',
                 type = 'Folder',
                 path = 'childEntity',
                 selectSet = [selection_spec('f_to_f'),
                              selection_spec('dc_to_hf'),
                              selection_spec('dc_to_vmf'),
                              selection_spec('dc_to_nf'),
                              selection_spec('dc_to_dsf'),
                              selection_spec('cr_to_h'),
                              selection_spec('cr_to_ds'),
                              selection_spec('cr_to_rp'),
                              selection_spec('h_to_vm'),
                              selection_spec('rp_to_vm')])
            ]
        traversal_specs = []
        for traversal in traversals:
            traversal_spec = self.create_object('TraversalSpec')
            for k,v in traversal.iteritems():
                setattr(traversal_spec, k, v)
            traversal_specs.append(traversal_spec)
        return traversal_specs


class ManagedObjectReference(suds.sudsobject.Property):
    """ Custom class hack to augment Property with _type """
    def __init__(self, _type, value):
        suds.sudsobject.Property.__init__(self, value)
        self._type = _type


class ManagedObject(object):
    def __init__(self, mor, vim, properties=None):
        self.mor = mor
        self.vim = vim
        if properties:
            self.update_local_view(properties)

    def update_local_view(self, properties=None):
        """
        Update the local version of the specified properties from the server

        @param properties: list of property names to update

        @return: True on success, False otherwise
        """
        assert properties, "properties must be specified"
        # Specify which properties we want
        # TODO: could do an 'all' here if needed
        property_spec = self.vim.create_object('PropertySpec')
        property_spec.type = str(self.mor._type)
        property_spec.all = False
        property_spec.pathSet = properties
        object_spec = self.vim.create_object('ObjectSpec')
        object_spec.obj = self.mor
        pfs = self.vim.create_object('PropertyFilterSpec')
        pfs.propSet = [property_spec]
        pfs.objectSet = [object_spec]
        object_contents = self.vim.invoke('RetrieveProperties',
                                          _this=self.vim.property_collector,
                                          specSet=pfs)
        if len(object_contents) == 1:
            self.update_object(object_contents[0])
            return True
        else:
            return False

    def update_object(self, object_content):
        """
        Update the object from an object content response from the server

        @param object_content: object content to update with
        """
        for prop in getattr(object_content, 'propSet', []):
            if prop.val.__class__.__name__.startswith('Array'):
                # suds embeds Array-type data into lists
                setattr(self, prop.name, prop.val[0])
            else:
                setattr(self, prop.name, prop.val)


class VirtualMachine(ManagedObject):
    def power_state(self):
        if not getattr(self, 'summary'):
            self.update_local_view(['summary'])
        return self.summary.runtime.powerState

    def power_on(self):
        return self.vim.wait_for_task(self.power_on_task())

    def power_on_task(self):
        return ManagedObject(self.vim.invoke('PowerOnVM_Task', _this=self.mor), vim=self.vim)

    def power_off(self):
        return self.vim.wait_for_task(self.power_off_task())

    def power_off_task(self):
        return ManagedObject(self.vim.invoke('PowerOffVM_Task', _this=self.mor), vim=self.vim)

    def clone_vm(self, clonename=None, linked_clone=False):
        """
        Create a full or linked clone of the VM

        @note: see clone_vm_task()
        """
        return self.vim.wait_for_task(self.clone_vm_task(clonename, linked_clone))

    def clone_vm_task(self, clonename=None, linked_clone=False, resource_pool=None, datastore=None, folder=None, cluster=None):
        """
        Create a full or linked clone of the VM

        @param clonename: name of the clone (make sure it does not exist yet)
        @param linked_clone: set True for linked clones
        @param resource_pool: name or ManagedObject, defaults to inherit from the base VM
        @param datastore: name or ManagedObject, defaults to inherit from the base VM
        @param folder: folder to place the VM
        @param cluster: compute resource whose resource pool to place the VM

        @notes: The clone is created on the same data store and host as its parent
        """
        assert clonename, "clonename needs to be specified"

        self.update_local_view(properties=['parent', 'datastore', 'resourcePool'])
        if datastore:
            clone_datastore = datastore if isinstance(datastore, ManagedObject) else self.vim.find_entity_by_name('Datastore', datastore)
        else:
            clone_datastore = ManagedObject(mor=self.datastore[0], vim=self)
        assert clone_datastore, "Datastore not set for the clone. The name %s may be incorrect" % str(datastore)
        if resource_pool:
            clone_resource_pool = resource_pool if isinstance(resource_pool, ManagedObject) else self.vim.find_entity_by_name('ResourcePool', resource_pool)
            assert clone_resource_pool, "resource pool %r not found" % resource_pool
            clone_resource_pool = clone_resource_pool.mor
        else:
            if cluster:
                compute_resource = self.vim.find_entity_by_name('ComputeResource', cluster, ['name', 'resourcePool'])
                if not compute_resource:
                    raise InvalidParameter("cluster %r not found" % cluster)
                clone_resource_pool = compute_resource.resourcePool
            else:
                # If neither the resource pool nor the cluster has been specified try to autodetect
                # by finding a single root resource pool. If none or more than one found, bail.
                resource_pools = [x for x in self.vim.find_entities_by_type('ResourcePool', ['parent'])
                                  if 'ComputeResource' in x.parent._type]
                if len(resource_pools) != 1:
                    raise InvalidParameter("root resource pool could not be determined unambiguously, specify the 'cluster' parameter")
                clone_resource_pool = resource_pools[0].mor
        if folder:
            target_folder = self.vim.invoke('FindByInventoryPath', _this=self.vim.service_content.searchIndex, inventoryPath=folder)
            if not target_folder:
                raise InvalidParameter("specified target folder %r not found" % folder)
        else:
            target_folder = self.parent

        relspec = self.vim.create_object('VirtualMachineRelocateSpec')
        relspec.host = None # Leave the host selection to vSphere
        relspec.pool = None
        relspec.pool = clone_resource_pool
        relspec.datastore = clone_datastore.mor
        relspec.transform = None
        if linked_clone:
            relspec.diskMoveType = "moveChildMostDiskBacking"
        clonespec = self.vim.create_object('VirtualMachineCloneSpec')
        clonespec.location = relspec
        clonespec.powerOn = "0"
        clonespec.template = "0"
        clonespec.snapshot = None
        task_mor = self.vim.invoke('CloneVM_Task', _this=self.mor, name=clonename, spec=clonespec, folder=target_folder)
        task = ManagedObject(mor=task_mor, vim=self.vim)
        return task

    def delete_vm(self):
        return self.vim.wait_for_task(self.delete_vm_task())

    def delete_vm_task(self):
        return ManagedObject(self.vim.invoke('Destroy_Task', _this=self.mor), vim=self.vim)

    def create_snapshot(self, name, description=None, memory=False, quiesce=False):
        return self.vim.wait_for_task(self.create_snapshot_task(name=name, description=description,
                                                                memory=memory, quiesce=quiesce))

    def create_snapshot_task(self, name, description=None, memory=False, quiesce=False):
        return ManagedObject(self.vim.invoke('CreateSnapshot_Task', _this=self.mor, name=name,
                                             description=description, memory=memory, quiesce=quiesce), vim=self.vim)

    def revert_to_current_snapshot(self):
        return self.vim.wait_for_task(self.revert_to_current_snapshot_task())

    def revert_to_current_snapshot_task(self):
        return ManagedObject(self.vim.invoke('RevertToCurrentSnapshot_Task', _this=self.mor), vim=self.vim)

    def list_snapshots(self):
        """ Return all snapshots as VirtualMachineSnapshotTree objects """
        def collect_snapshots(snapshot_list):
            snapshots = []
            for snap in snapshot_list:
                snapshots.append(snap)
                child_list = getattr(snap, 'childSnapshotList', None)
                if child_list:
                    snapshots.extend(collect_snapshots(child_list))
            return snapshots

        self.update_local_view(properties=['snapshot'])
        if getattr(self, 'snapshot', None):
            snapshots = collect_snapshots(self.snapshot.rootSnapshotList)
            # Swap out the snapshot references to directly-usable snapshot objects
            for snapshot in snapshots:
                snapshot.snapshot = VirtualMachineSnapshot(mor=snapshot.snapshot, vim=self.vim)
            return snapshots
        else:
            return []

    def find_snapshots_by_name(self, name):
        return [snapshot for snapshot in self.list_snapshots() if snapshot.name == name]

    def run_script_in_guest(self, script, username, password, shell='/bin/bash'):
        """
        Run a script in the guest VM

        @param script: script text to run
        @param username: existing user on the system
        @param password: password of the user
        @param shell: shell to execute the script, defaults to '/bin/bash'

        @returns: process ID of the script run in the guest
        """
        assert hasattr(self.vim.service_content, 'guestOperationsManager'), 'vSphere version 5.0 or later is needed for guest operations'
        auth = self.vim.create_object('NamePasswordAuthentication')
        auth.username = username
        auth.password = password
        auth.interactiveSession = False
        guest_manager = ManagedObject(mor=self.vim.service_content.guestOperationsManager, vim=self.vim, properties=['fileManager', 'processManager'])
        temp_path = self.vim.invoke('CreateTemporaryFileInGuest', _this=guest_manager.fileManager, vm=self.mor, auth=auth, prefix='', suffix='')
        attr = self.vim.create_object('GuestFileAttributes')
        # Use default file attributes
        attr.accessTime = None
        attr.modificationTime = None
        attr.symlinkTarget = None
        upload_url = self.vim.invoke('InitiateFileTransferToGuest', _this=guest_manager.fileManager, vm=self.mor, auth=auth, guestFilePath=temp_path, fileAttributes=attr, fileSize=len(script), overwrite=True)
        assert not '*' in upload_url, "'http://*/guestFile?id=1&token=1234'-style upload URLs are not supported yet: %r" % upload_url
        # This hack makes urllib2 to issue a PUT request that vSphere wants for file uploads
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(upload_url, data=script)
        request.add_header('Content-Type', 'text/plain')
        request.get_method = lambda: 'PUT'
        url = opener.open(request)
        program_spec = self.vim.create_object('GuestProgramSpec')
        program_spec.arguments = temp_path
        program_spec.envVariables = None
        program_spec.programPath = shell
        program_spec.workingDirectory = None
        pid = self.vim.invoke('StartProgramInGuest', _this=guest_manager.processManager, vm=self.mor, auth=auth, spec=program_spec)
        self.vim.invoke('DeleteFileInGuest', _this=guest_manager.fileManager, vm=self.mor, auth=auth, filePath=temp_path)
        return pid

    def reconfig_vm(self, spec):
        """
        Change VM configuration settings accoding to 'spec'

        @param spec: VirtualMachineConfigSpec type
        """
        return self.vim.wait_for_task(self.reconfig_vm_task(spec=spec))

    def reconfig_vm_task(self, spec):
        """
        Change VM configuration settings accoding to 'spec'

        @param spec: VirtualMachineConfigSpec type
        """
        return ManagedObject(self.vim.invoke('ReconfigVM_Task', _this=self.mor, spec=spec), vim=self.vim)

    def spec_new_disk(self, size, thin=True, disk_mode='persistent'):
        """
        Prepare a device config spec for a new virtual disk (for reconfig_vm())

        @param size: in megabytes
        @param thin: thin provisioning (set to False for thick)
        @param disk_mode: see VirtualDiskMode in the vSphere API documentation

        @note: this method requires at least one disk to be present already
        """
        disk_modes = [ "persistent", "independent_persistent", "independent_nonpersistent", "nonpersistent", "undoable", "append" ]
        assert disk_mode in disk_modes, "disk mode must be one of '%s', not %s" % (", ".join(disk_modes), disk_mode)

        if not hasattr(self, 'config'):
            assert self.update_local_view(properties=['config']), "failed to update the 'config'property of the VM"
        # Find the virtual disk controller and its key
        disk_controllers = [x for x in self.config.hardware.device if x.__class__.__name__ == 'VirtualLsiLogicController']
        assert disk_controllers, "could not find virtual disk controller 'VirtualLsiLogicController'"
        controller_key = disk_controllers[0].key
        # Find a unit number for the new disk
        virtual_disks = [x for x in self.config.hardware.device if x.__class__.__name__ == 'VirtualDisk']
        assert virtual_disks, "this method requires at least one disk to be already attached to the VM"
        new_disk_unit_number = max([x.unitNumber for x in virtual_disks if x.controllerKey == controller_key]) + 1

        backing = self.vim.create_object('VirtualDiskFlatVer2BackingInfo')
        backing.datastore = virtual_disks[0].backing.datastore
        backing.fileName = "" # File name chosen by vSphere
        backing.eagerlyScrub = False
        backing.thinProvisioned = thin
        backing.diskMode = disk_mode
        disk = self.vim.create_object('VirtualDisk')
        disk.controllerKey = controller_key
        disk.key = None
        disk.unitNumber = new_disk_unit_number
        disk.capacityInKB = size * 1024
        disk.backing = backing
        file_op_enum = self.vim.create_object('VirtualDeviceConfigSpecFileOperation')
        spec_enum = self.vim.create_object('VirtualDeviceConfigSpecOperation')
        device_config_spec = self.vim.create_object('VirtualDeviceConfigSpec')
        device_config_spec.device = disk
        device_config_spec.fileOperation = file_op_enum.create
        device_config_spec.operation = spec_enum.add
        return device_config_spec

    def spec_new_nic(self, network, nic_type="vmxnet2"):
        """
        Prepare a device config spec for a new virtual NIC (for reconfig_vm())
        """
        NIC_TYPES = { "e1000":   "VirtualE1000",
                      "pcnet32": "VirtualPCNet32",
                      "vmxnet2": "VirtualVmxnet2",
                      "vmxnet3": "VirtualVmxnet3" }
        assert nic_type in NIC_TYPES, "nic_type must be one of %s" % ', '.join(NIC_TYPES)

        backing = self.vim.create_object('VirtualEthernetCardNetworkBackingInfo')
        backing.deviceName = network
        backing.network = None
        nic = self.vim.create_object(NIC_TYPES[nic_type])
        nic.backing = backing
        nic.key = None
        spec_enum = self.vim.create_object('VirtualDeviceConfigSpecOperation')
        device_config_spec = self.vim.create_object('VirtualDeviceConfigSpec')
        device_config_spec.device = nic
        device_config_spec.operation = spec_enum.add
        device_config_spec.fileOperation = None
        return device_config_spec


class VirtualMachineSnapshot(ManagedObject):
    def rename_snapshot(self, name=None, description=None):
        assert name or description, "at least one of 'name' and 'description' must be supplied"
        self.vim.invoke('RenameSnapshot', _this=self.mor, name=name, description=description)

    def remove_snapshot(self, remove_children=False):
        return self.vim.wait_for_task(self.remove_snapshot_task(remove_children=remove_children))

    def remove_snapshot_task(self, remove_children=False):
        return ManagedObject(self.vim.invoke('RemoveSnapshot_Task', _this=self.mor, removeChildren=remove_children), vim=self.vim)

    def revert_to_snapshot(self, suppress_power_on=False):
        return self.vim.wait_for_task(self.revert_to_snapshot_task(suppress_power_on=suppress_power_on))

    def revert_to_snapshot_task(self, suppress_power_on=False):
        return ManagedObject(self.vim.invoke('RevertToSnapshot_Task', _this=self.mor, suppressPowerOn=suppress_power_on), vim=self.vim)

    def __eq__(self, other):
        return self.mor._type == other.mor._type and self.mor.value == other.mor.value
