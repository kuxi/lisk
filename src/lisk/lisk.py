from __future__ import division

import os
import stat
import errno
import threading
from time import time, mktime, strptime, sleep

import llfuse

from nodes import Node, FileNode
from channels import file_channel
import utils


class Lisk(llfuse.Operations):
    FILE_MODE = stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR |\
        stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH
    DIR_MODE = stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR |\
        stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP |\
        stat.S_IROTH | stat.S_IXOTH

    def __init__(self):
        super(Lisk, self).__init__()
        rootentry = llfuse.EntryAttributes()
        rootentry.st_ino = llfuse.ROOT_INODE
        rootentry.st_mode = stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR |\
            stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP |\
            stat.S_IROTH | stat.S_IXOTH
        rootentry.st_uid = os.getuid()
        rootentry.st_gid = os.getgid()
        rootentry.st_atime = time()
        rootentry.st_mtime = time()
        rootentry.st_ctime = time()
        rootentry.st_rdev = 0
        rootentry.st_size = 0
        rootentry.generation = 0
        rootentry.entry_timeout = 300
        rootentry.attr_timeout = 300
        rootentry.st_nlink = 1
        rootentry.st_blksize = 512
        rootentry.st_blocks = 1
        self.ROOT_NODE = Node(llfuse.ROOT_INODE, rootentry, b'..',
                              Node.ROOT, None)
        self.nodes = {
            llfuse.ROOT_INODE: self.ROOT_NODE
        }
        self.inode_number = self.ROOT_NODE.inode

    def _create_dummy_attr(self, inode):
        entry = llfuse.EntryAttributes()
        entry.st_ino = inode
        entry.st_mode = stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR |\
            stat.S_IRGRP | stat.S_IROTH
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_atime = time()
        entry.st_mtime = time()
        entry.st_ctime = time()
        entry.st_rdev = 0
        entry.st_size = 0
        entry.generation = 0
        entry.entry_timeout = 300
        entry.attr_timeout = 300
        entry.st_nlink = 1
        entry.st_blksize = 512
        entry.st_blocks = 1
        return entry
        self.nodes

    def lookup(self, inode_p, name):
        print 'entering lookup', inode_p, name
        parent = self.nodes[inode_p]
        if name == ".":
            node = parent
        elif name == "..":
            node = parent.parent
        else:
            try:
                inode = filter(lambda i: self.nodes[i].name == name and
                               self.nodes[i].parent == parent, self.nodes)[0]
                node = self.nodes[inode]
            except Exception as e:
                print 'raising', e
                raise llfuse.FUSEError(errno.ENOENT)
        print 'returning lookup', node
        return self.getattr(node.inode)

    def getattr(self, inode):
        print 'entering getattr', inode
        entry = self.nodes[inode].entry
        return entry

    def getxattr(self, inode, name):
        print 'entering getxattr', inode, name
        raise llfuse.FUSEError(llfuse.ENOATTR)

    def setattr(self, inode, attr):
        print 'entering setattr', inode, attr
        node = self.nodes[inode]
        if attr.st_size is not None:
            print 'setting st_size', attr.st_size
            node.entry.st_size = attr.st_size
        if attr.st_mode is not None:
            print 'setting st_mode', attr.st_mode
            node.entry.st_mode = attr.st_mode
        if attr.st_uid is not None:
            print 'setting st_uid', attr.st_uid
            node.entry.st_uid = attr.st_uid
        if attr.st_gid is not None:
            print 'setting st_gid', attr.st_gid
            node.entry.st_gid = attr.st_gid
        if attr.st_rdev is not None:
            print 'setting st_rdev', attr.st_rdev
            node.entry.st_rdev = attr.st_rdev

        return self.getattr(inode)

    def can_create(self, child_type, parent_type):
        if child_type == Node.FILE:
            if parent_type != Node.PROJECT:
                # TODO: allow moving files to filespaces
                #Only allow moving files to projects
                return False
            return True
        elif child_type == Node.PROJECT:
            if parent_type != Node.SPACE:
                return False
            return True
        return False

    def rename(self, parent_old_i, old_name, parent_new_i, new_name):
        print 'renaming', parent_old_i, old_name, parent_new_i, new_name
        parent_new = self.nodes[parent_new_i]
        parent_old = self.nodes[parent_old_i]
        if self.getattr(parent_new_i).st_nlink == 0:
            print 'Attempted to create entry %s with unlinked parent %d' % (
                new_name, parent_new_i)
            raise llfuse.FUSEError(errno.EINVAL)
        try:
            old_node_attr = self.lookup(parent_old_i, old_name)
        except llfuse.FUSEError:
            print 'Attempted to move nonexistant file %s', old_name
            raise
        old_node = self.nodes[old_node_attr.st_ino]
        if not self.can_create(old_node.type, parent_new.type):
            raise llfuse.FUSEError(errno.EINVAL)

        if old_node.type == Node.PROJECT:
            utils.put_project(old_node.doc_id, {
                'title': new_name,
                'space': parent_new.doc_id
            })
        elif old_node.type == Node.FILE:
            parent_type = 'project'
            if parent_new.type == Node.SPACE:
                parent_type = 'space'
            options = {'title': new_name}
            options[parent_type] = parent_new.doc_id
            utils.put_file(old_node.doc_id, options)

        old_node.name = new_name
        old_node.parent = parent_new
        parent_new.cached = False
        parent_old.cached = False

    def opendir(self, inode):
        print 'opendir', inode
        return inode

    def _make_node(self, name, parent, node_type, doc_id, **attrs):
        self.inode_number += 1
        entry = llfuse.EntryAttributes()
        entry.st_ino = self.inode_number
        entry.st_mode = attrs.get('st_mode', self.FILE_MODE)
        entry.st_uid = attrs.get('st_uid', os.getuid())
        entry.st_gid = attrs.get('st_gid', os.getgid())
        entry.st_atime = attrs.get('st_atime', time())
        entry.st_mtime = attrs.get('st_mtime', time())
        entry.st_ctime = attrs.get('st_ctime', time())
        entry.st_rdev = attrs.get('st_rdev', 0)
        entry.st_size = attrs.get('st_size', 0)
        entry.generation = attrs.get('generation', 0)
        entry.entry_timeout = attrs.get('entry_timeout', 300)
        entry.attr_timeout = attrs.get('attr_timeout', 300)
        entry.st_nlink = attrs.get('st_nlink', 1)
        entry.st_blksize = attrs.get('st_blksize', 512)
        entry.st_blocks = attrs.get('st_blocks', 1)
        if node_type == Node.FILE:
            node = FileNode(
                self.inode_number, entry, name, node_type, doc_id, parent)
        else:
            node = Node(
                self.inode_number, entry, name, node_type, doc_id, parent)
        return node

    def _make_json_transformer(self, parent, st_mode):
        def transformer(obj):
            if obj['type'] == 'Space':
                node_type = Node.SPACE
            elif obj['type'] == 'Project':
                node_type = Node.PROJECT
            elif obj['type'] == 'File':
                node_type = Node.FILE
            else:
                raise Exception("Unsupported document type")
            date_format = '%Y-%m-%dT%H:%M:%S'
            cdate = strptime(obj['created'], date_format)
            mdate = strptime(obj['modified'], date_format)

            node = self._make_node(obj['title'].encode('utf-8'),
                                   parent,
                                   node_type,
                                   obj['id'],
                                   st_ctime=mktime(cdate),
                                   st_mtime=mktime(mdate),
                                   st_mode=st_mode)
            return node
        return transformer

    def readdir(self, fh, off):
        print 'readdir', fh, off
        parent = self.nodes[fh]
        # TODO: this won't work properly when offset is used
        #       If I want to cache like this I'll need to get all children of
        #       the parent before marking it as cached, not from some offset
        #       and there is no guarantee that this generator will be
        #       exhausted, meaning the cached flag won't be set
        if parent.cached:
            children = filter(lambda i: self.nodes[i].parent.inode == fh,
                              self.nodes)
            generator = (self.nodes[f] for f in children[off:])
        else:
            if parent.type == Node.ROOT:
                #Working with root, get all spaces
                transformer = self._make_json_transformer(
                    self.ROOT_NODE, self.DIR_MODE)
                generator = utils.get_spaces(transformer, offset=off)
            elif parent.type == Node.SPACE:
                #working with a space, get its projects
                # TODO: handle filespaces
                transformer = self._make_json_transformer(
                    parent, self.DIR_MODE)
                generator = utils.get_space_projects(transformer,
                                                     parent.doc_id, offset=off)
            elif parent.type == Node.PROJECT:
                #working with a project, get its files
                transformer = self._make_json_transformer(
                    parent, self.FILE_MODE)
                generator = utils.get_project_files(transformer, parent.doc_id,
                                                    offset=off)
            else:
                # shouldn't happen
                print 'trying to read non-folder type document', parent.type
                generator = ()
        for i, node in enumerate(generator):
            print 'yielding', node.name, node.entry, off + i + 1
            if node.inode not in self.nodes:
                self.nodes[node.inode] = node
            yield (node.name, self.getattr(node.inode), off + i + 1)
        parent.cached = True

    def statfs(self):
        print 'statfs'
        stat_ = llfuse.StatvfsData()

        stat_.f_bsize = 512
        stat_.f_frsize = 512

        size = sum(self.nodes[i].entry.st_size for i in self.nodes)
        stat_.f_blocks = size // stat_.f_frsize
        stat_.f_bfree = max(size // stat_.f_frsize, 1024)
        stat_.f_bavail = stat_.f_bfree

        inodes = len(self.nodes)
        stat_.f_files = inodes
        stat_.f_ffree = max(inodes, 100)
        stat_.f_favail = stat_.f_ffree

        print 'returning statfs', stat_
        return stat_

    def open(self, inode, flags):
        print 'open', inode, flags
        return inode

    def access(self, inode, mode, ctx):
        print 'access', inode, mode, ctx
        return True

    def read(self, fh, offset, length):
        print 'entering read', fh, offset, length
        data = self.nodes[fh].content
        if data is None:
            data = ''
        print 'returning read', data
        return data[offset:offset + length]

    def write(self, fh, offset, buf):
        #TODO: Assumes that files will be written to sequentially
        #      - ignores the offset!!
        print 'entering write', fh, offset, buf
        node = self.nodes[fh]
        file_channel.write(buf, node)

        print 'exiting write'
        #lying a bit here...
        return len(buf)

    def mkdir(self, inode_p, name, mode, ctx):
        print 'mkdir', inode_p, name, mode, ctx
        parent_node = self.nodes[inode_p]
        if self.getattr(inode_p).st_nlink == 0:
            print 'Attempted to create entry %s with unlinked parent %d' % (
                name, inode_p)
            raise llfuse.FUSEError(errno.EINVAL)

        if parent_node.type == Node.ROOT:
            node_type = Node.SPACE
            # Can't create spaces via api
            raise llfuse.FUSEError(errno.EINVAL)
        else:
            node_type = Node.PROJECT
            res = utils.post_project({
                'title': name,
                'space': parent_node.doc_id
            })
        doc_id = res.headers['location'].rsplit('/', 2)[1]

        node = self._make_node(
            name, parent_node, node_type, doc_id, st_mode=mode,
            st_ctime=time(), st_mtime=time(), st_atime=time(), st_uid=ctx.uid,
            st_gid=ctx.gid)
        self.nodes[node.inode] = node

        return node.entry

    def create(self, inode_p, name, mode, flags, ctx):
        parent_node = self.nodes[inode_p]
        if self.getattr(inode_p).st_nlink == 0:
            print 'Attempted to create entry %s with unlinked parent %d' % (
                name, inode_p)
            raise llfuse.FUSEError(errno.EINVAL)

        if not self.can_create(Node.FILE, parent_node.type):
            raise llfuse.FUSEError(errno.EINVAL)
        res = utils.post_file({
            'filename': name,
            'title': name,
            'project': parent_node.doc_id,
        })
        #yeah...
        doc_id = res.headers['location'].rsplit('/', 2)[1]

        node = self._make_node(
            name, parent_node, Node.FILE, doc_id, st_mode=mode,
            st_ctime=time(), st_mtime=time(), st_atime=time(), st_uid=ctx.uid,
            st_gid=ctx.gid)
        self.nodes[node.inode] = node

        return (node.inode, node.entry)

    def forget(self, inode_list):
        print 'entering forget', inode_list
        for inode, nlookup in inode_list:
            del self.nodes[inode]


class FileWorker(threading.Thread):
    #Works, but might be better to make a file-like object
    #since the requests lib can stream uploads like that
    #(instead of 'unbuffering' the writes)
    #would still need to time out though
    def __init__(self, *args, **kwargs):
        super(FileWorker, self).__init__(*args, **kwargs)
        self.daemon = True

    def flush(self, f, data):
        f.content = data

    def run(self, *args, **kwargs):
        last_file = None
        total_data = None
        while True:
            result = file_channel.read()
            if result:
                #Something was written since our last poll
                data, f = result
                if last_file == f:
                    #We're writing to the same file as last write
                    #assume that we're appending
                    total_data += data
                elif last_file:
                    #We're writing to a different file than in
                    #last write.
                    #Assume that we're starting a write to a new file
                    #flush what we have to finish previous file
                    self.flush(last_file, total_data)
                    total_data = data
                else:
                    #No previous file, start new total_data buffer
                    total_data = data
                last_file = f
            else:
                #No data has been written since last poll
                if total_data and last_file:
                    #Last poll contained some data
                    #This means that the write has timed out
                    #and we should flush what we have
                    self.flush(last_file, total_data)
                total_data = None
                last_file = None
            sleep(1)


def run():
    fs = Lisk()
    fw = FileWorker()
    llfuse.init(fs, "/home/heidar/fs", [b"fsname=lisk", b"nonempty"])
    print "here"
    print llfuse.ROOT_INODE

    fw.start()
    try:
        llfuse.main(single=True)
    except:
        llfuse.close()
        raise

    llfuse.close()
