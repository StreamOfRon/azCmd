#!/usr/bin/env python

import azure, os, sys

class azCmd:
	_service = None
	
	def __init__(self, account, key):
		self.connect(account, key)
		
	def connect(self, account, key):
		if self._service == None:
			from azure.storage import BlobService
			self._service = BlobService(account, key)
			
	def mkdir(self, container_name):
		container = self._get_container_name(container_name)
		return self._service.create_container(container)
	
	def list_containers(self):
		return self._service.list_containers()
	
	def rmdir(self, container_name):
		container_name = container_name.rstrip('/') + '/'
		container = self._get_container_name(container_name)
		print "Container: " + container
		if container == container_name:
			return self._service.delete_container(container)
		else:
			blobprefix = self._get_blob_prefix(container_name)
			print "Blobprefix: " + blobprefix
			blobs = self._service.list_blobs(container, blobprefix)
			for blob in blobs:
				self._service.delete_blob(container, blob.name)
	
	def put(self,local,remote):
		container = self._get_container_name(remote)
		blobname = self._get_blob_name(remote)
		try:
			self._service.put_block_blob_from_path(container, blobname, local)
		except (azure.WindowsAzureMissingResourceError):
			self.mkdir(container)
			self._service.put_block_blob_from_path(container, blobname, local)
		
	def get(self,remote,local):
		container = self._get_container_name(remote)
		blobname = self._get_blob_name(remote)
		self._service.get_blob_to_path(container,blobname,local)
		
	def ls(self,path):
		path = path.strip('/') + '/'
		container = self._get_container_name(path)
		prefix = self._get_blob_prefix(path)
		bloblist = list()
		lastPrefix = None
		for blob in self._service.list_blobs(container, prefix).blobs:
			blobprefix = self._get_blob_prefix("%s/%s" % (container, blob.name))	
			if blobprefix == None:
				if prefix != None:
					blob.name = blob.name[len(prefix)+1:]
				setattr(blob, 'directory', False)
				bloblist.append(blob)
			else:
				if blobprefix == prefix:
					blob.name = blob.name[len(prefix)+1:]
					setattr(blob, 'directory', False)
					bloblist.append(blob)
				elif blobprefix != lastPrefix:
					if prefix != None:
						thisprefix = blobprefix[len(prefix)+1:]
					else:
						thisprefix = blobprefix
					if len(thisprefix.split('/')) < 2:
						blob.name = thisprefix
						setattr(blob, 'directory', True)
						bloblist.append(blob)
				lastPrefix = blobprefix
				
		return bloblist
	
	def rm(self, remote):
		container = self._get_container_name(remote)
		blobname = self._get_blob_name(remote)
		self._service.delete_blob(container, blobname)
	
	def _parse_modification_time(self, datestring):
		from datetime import datetime
		date = datetime.strptime(datestring, "%a, %d %b %Y %H:%M:%S %Z")
		return date.strftime("%b %d %H:%M")
	
	def _get_container_name(self,path):
		container, sep, blobname = path.partition('/')
		return container
		
	def _get_blob_prefix(self,path):
		print "Prefix for: " + path
		blobname = self._get_blob_name(path)
		prefix, sep, filename = blobname.rpartition('/')
		if len(prefix) > 0:
			return prefix
		else:
			return None
		
	def _get_blob_name(self,path):
		container, sep, blobname = path.partition('/')
		return blobname
	
if __name__ == '__main__':
	iface = azCmd(os.environ.get('AZURE_STORAGE_ACCOUNT'), os.environ.get('AZURE_STORAGE_ACCESS_KEY'))
	if sys.argv[1] == 'mkdir':
		iface.mkdir(sys.argv[2])
	elif sys.argv[1] == 'lsdir':
		containers = iface.list_containers()
		for container in containers:
			#date = datetime.strptime(container.properties.last_modified, "%a 	")
			print "drwxrwxrwx root root - %s %s" % (iface._parse_modification_time(container.properties.last_modified), iface._parse_container_name_incoming(container.name))
	elif sys.argv[1] == 'rmdir':
		iface.rmdir(sys.argv[2])
	elif sys.argv[1] == 'put':
		iface.put(sys.argv[2], sys.argv[3])
	elif sys.argv[1] == 'get':
		iface.get(sys.argv[2], sys.argv[3])
	elif sys.argv[1] == 'rm':
		iface.rm(sys.argv[2])
	elif sys.argv[1] == 'delete':
		iface.rm(sys.argv[2])
	elif sys.argv[1] == 'ls':
		blobs = iface.ls(sys.argv[2])
		for blob in blobs:
			if blob.directory:
				char = "d"
				blob.name = blob.name + '/'
			else:
				char = "-"
			print "%srwxrwxrwx root root %s %s %s" % (char, blob.properties.content_length, iface._parse_modification_time(blob.properties.last_modified), blob.name)
	elif sys.argv[1] == 'chdir':
		print sys.argv[2]
	else:
		print "Unknown command"
	