#!/usr/bin/env python
from __future__ import print_function

from datetime import datetime

import argparse
import os

import azure


class AzCmd(object):
    _service = None

    def __init__(self, account, key):
        self.connect(account, key)

    def connect(self, account, key):
        if self._service is None:
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
        print("Container: " + container)
        if container == container_name:
            return self._service.delete_container(container)
        else:
            blobprefix = self._get_blob_prefix(container_name)
            print("Blobprefix: " + blobprefix)
            blobs = self._service.list_blobs(container, blobprefix)
            for blob in blobs:
                self._service.delete_blob(container, blob.name)

    def put(self, local, remote):
        container = self._get_container_name(remote)
        blobname = self._get_blob_name(remote)
        try:
            self._service.put_block_blob_from_path(container, blobname, local)
        except (azure.WindowsAzureMissingResourceError):
            self.mkdir(container)
            self._service.put_block_blob_from_path(container, blobname, local)

    def get(self, remote, local):
        container = self._get_container_name(remote)
        blobname = self._get_blob_name(remote)
        self._service.get_blob_to_path(container, blobname, local)

    def ls(self, path):
        path = path.strip('/') + '/'
        container = self._get_container_name(path)
        prefix = self._get_blob_prefix(path)
        bloblist = list()
        lastPrefix = None
        for blob in self._service.list_blobs(container, prefix).blobs:
            blobprefix = self._get_blob_prefix("%s/%s" % (container, blob.name))
            if blobprefix is None:
                if prefix is not None:
                    blob.name = blob.name[len(prefix) + 1:]
                setattr(blob, 'directory', False)
                bloblist.append(blob)
            else:
                if blobprefix == prefix:
                    blob.name = blob.name[len(prefix) + 1:]
                    setattr(blob, 'directory', False)
                    bloblist.append(blob)
                elif blobprefix != lastPrefix:
                    if prefix is not None:
                        thisprefix = blobprefix[len(prefix) + 1:]
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

    def _get_container_name(self, path):
        container, sep, blobname = path.partition('/')
        return container

    def _get_blob_prefix(self, path):
        print("Prefix for: " + path)
        blobname = self._get_blob_name(path)
        prefix, sep, filename = blobname.rpartition('/')
        if len(prefix) > 0:
            return prefix
        else:
            return None

    def _get_blob_name(self, path):
        container, sep, blobname = path.partition('/')
        return blobname


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Python CLI for Microsoft Azure Storage")

    # flags
    parser.add_argument('-a', '--storage-account',
                        default=os.getenv('AZURE_STORAGE_ACCOUNT'),
                        help="Azure storage account UUID. Read from the environment variable AZURE_STORAGE_ACCOUNT if not supplied.")
    parser.add_argument('-k', '--access-key',
                        default=os.getenv('AZURE_STORAGE_ACCESS_KEY'),
                        help="Access key for the specified storage account. Read from the environment variable AZURE_STORAGE_ACCESS_KEY if not supplied.")

    # command subparsers
    commands = parser.add_subparsers(dest='cmd')
    mkdir = commands.add_parser('mkdir', help='Create a new remote directory at [remote]')
    mkdir.add_argument('remote', type=str, help='A path to a remote file')

    lsdir = commands.add_parser('lsdir', help='List all containers in the specified account')

    rmdir = commands.add_parser('rmdir', help='Remove the remote directory at [remote]')
    rmdir.add_argument('remote', type=str, help='A path to a remote file')

    put = commands.add_parser('put', help='Upload the contents of the file [local] into [remote]')
    put.add_argument('local', type=str, help='A path to a local file')
    put.add_argument('remote', type=str, help='A path to a remote file')

    get = commands.add_parser('get', help='Download the contents of [remote] into the file at [local]')
    get.add_argument('remote', type=str, help='A path to a remote file')
    get.add_argument('local', type=str, help='A path to a local file')

    delete = commands.add_parser('delete', help='Delete the file at [remote]')
    delete.add_argument('remote', type=str, help='A path to a remote file')

    ls = commands.add_parser('ls', help='List the contents of the directory path [remote]')
    ls.add_argument('remote', type=str, help='A path to a remote file')

    chdir = commands.add_parser('chdir', help='Change the current directory to [remote]')
    chdir.add_argument('remote', type=str, help='A path to a remote file')

    args = parser.parse_args()

    if None in [args.access_key, args.storage_account]:
        parser.error("Must specify an Azure storage account and access key.")

    iface = AzCmd(args.storage_account, args.access_key)

    if args.cmd == 'mkdir':
        iface.mkdir(args.remote)

    if args.cmd == 'lsdir':
        containers = iface.list_containers()
        for container in containers:
            print('drwxrwxrwx root root - {mtime:%b %d %H:%M} {name:s}'.format(
                mtime=datetime.strptime(container.properties.last_modified, "%a, %d %b %Y %H:%M:%S %Z"),
                name=iface._parse_container_name_incoming(container.name)))

    if args.cmd == 'rmdir':
        iface.rmdir(args.remote)

    if args.cmd == 'put':
        iface.put(args.local, args.remote)

    if args.cmd == 'get':
        iface.get(args.remote, args.local)

    if args.cmd == 'delete':
        iface.rm(args.remote)

    if args.cmd == 'ls':
        blobs = iface.ls(args.remote)
        for blob in blobs:
            print('{d:c}rwxrwxrwx root root {size:d} {mtime:%b %d %H:%M} {name:s}'.format(
                d='d' if blob.directory else '-',
                size=blob.properties.content_length,
                mtime=datetime.strptime(blob.properties.last_modified, "%a, %d %b %Y %H:%M:%S %Z"),
                name=blob.name))

    if args.cmd == 'chdir':
        print(args.remote)
