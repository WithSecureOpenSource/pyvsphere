# -*- coding: utf-8 -*-
#
# Python interface to VMware vSphere API
#
import httplib
import time
import suds

class TimeoutError(Exception):
    def __init__(self, error):
        self.error = error
    def __str__(self):
        return repr(self.error)

class ObjectNotFoundError(Exception):
    def __init__(self, error):
        self.error = error
    def __str__(self):
        return repr(self.error)

class TaskFailed(Exception):
    def __init__(self, error):
        self.error = error
    def __str__(self):
        return repr(self.error)

class Vim(object):
    """
    Interface class for VMware VIM API over SOAP
    """
    def __init__(self, url, debug=False):
        """
        @param url: URL to the vSphere server (eg.: https://foosphere/sdk)
        @param debug: Run in debug mode (very noisy)
        """
        import logging
        self.task_timeout = 600
        if debug:
            logging.basicConfig(level=logging.INFO)
            logging.getLogger('suds.client').setLevel(logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
            logging.getLogger('suds').setLevel(logging.INFO)

        self.soapclient = suds.client.Client(url+"/vimService.wsdl")
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
            return False

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
                raise TimeoutError, "task timed out after %d seconds" % self.tas

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
        self.invoke('Login', _this=self.service_content.sessionManager,
                    userName=username, password=password)

    def logout(self):
        self.invoke('Logout', _this=self.service_content.sessionManager)

    def find_entities_by_type(self, entity_type, properties=None):
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
        return result

    def find_entity_by_name(self, entity_type, entity_name, properties=None):
        entities = self.find_entities_by_type(entity_type, properties=properties)
        for e in entities:
            for x in e.propSet:
                found = [x for x in e.propSet if x.name == 'name' and x.val == entity_name]
            if found:
                return e
        return None

    def find_vm_by_name(self, vmname, properties=None):
        vm_entity = self.find_entity_by_name('VirtualMachine', vmname, properties=properties)
        if vm_entity:
            vm = VirtualMachine(vm_entity.obj, self)
            for prop in vm_entity.propSet:
                if prop.val.__class__.__name__.startswith('Array'):
                    # suds embeds Array-type data into lists
                    setattr(vm, prop.name, prop.val[0])
                else:
                    setattr(vm, prop.name, prop.val)
            return vm
        else:
            return None

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
    def __init__(self, mor, vim):
        self.mor = mor
        self.vim = vim

    def update_local_view(self, properties=None):
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
        if not object_contents:
            return False
        assert len(object_contents) == 1, "got multiple ObjectContent responses"
        if object_contents:
            for prop in object_contents[0].propSet:
                if prop.val.__class__.__name__.startswith('Array'):
                    # suds embeds Array-type data into lists
                    setattr(self, prop.name, prop.val[0])
                else:
                    setattr(self, prop.name, prop.val)
            return True
        else:
            return False


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

        @param: clonename: name of the clone (make sure it does not exist yet)
        @param: linked_clone: set True for linked clones

        @notes: The clone is created on the same data store and host as its parent
        """
        return self.vim.wait_for_task(self.clone_vm_task(clonename, linked_clone))

    def clone_vm_task(self, clonename=None, linked_clone=False):
        """
        Create a full or linked clone of the VM

        @param: clonename: name of the clone (make sure it does not exist yet)
        @param: linked_clone: set True for linked clones

        @notes: The clone is created on the same data store and host as its parent
        """
        assert clonename, "clonename needs to be specified"
        self.update_local_view(properties=['parent', 'summary', 'datastore', 'resourcePool'])
        relspec = self.vim.create_object('VirtualMachineRelocateSpec')
        clonespec = self.vim.create_object('VirtualMachineCloneSpec')
        host = ManagedObject(mor=self.summary.runtime.host, vim=self.vim)
        host.update_local_view(properties=['parent', 'datastore'])
        compute_resource = ManagedObject(mor=host.parent, vim=self.vim)
        compute_resource.update_local_view(['resourcePool'])
        # TODO: precise specification of the clone's placement
        #       could be implemented here. Right now the clone inherits
        #       from the parent and the rest is left to vSphere.
        relspec.host = None # Leave the host selection to vSphere
        relspec.pool = compute_resource.resourcePool
        relspec.datastore = host.datastore[-1]
        relspec.transform = None
        if linked_clone:
            relspec.diskMoveType = "moveChildMostDiskBacking"
        clonespec.location = relspec
        clonespec.powerOn = "0"
        clonespec.template = "0"
        clonespec.snapshot = None
        task_mor = self.vim.invoke('CloneVM_Task', _this=self.mor, name=clonename, spec=clonespec, folder=self.parent)
        task = ManagedObject(mor=task_mor, vim=self)
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
