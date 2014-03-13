from time import time

import llfuse

import utils


class Node(object):
    ROOT = 0
    SPACE = 1
    PROJECT = 2
    FILE = 3
    ALL_TYPES = [ROOT, SPACE, PROJECT, FILE]

    def __init__(self, inode, attributes,
                 name, node_type, doc_id, parent=None):
        assert(isinstance(attributes, llfuse.EntryAttributes))
        assert(node_type in Node.ALL_TYPES)
        if not parent:
            parent = self
        else:
            assert(isinstance(parent, Node))
        self.inode = inode
        self.entry = attributes
        self.name = name
        self.parent = parent
        self.entry.st_ino = inode
        self.type = node_type
        self.doc_id = doc_id


class FileNode(Node):
    def __init__(self, inode, attribute_entry, name,
                 node_type, doc_id, parent):
        super(FileNode, self).__init__(
            inode, attribute_entry, name, node_type, doc_id, parent)
        self._content = None
        self.entry.st_size = self.size

    def _get_content(self):
        self.entry.st_atime = time()
        if not self._content:
            self._content = utils.get_file_content(self.doc_id)
        return self._content

    def _set_content(self, content):
        print 'before', self.content, self.size
        utils.put_file_content(self.doc_id, content)
        self._content = content
        self.entry.st_size = self.size
        print 'after', self._content, self.size
        self.entry.st_mtime = time()

    content = property(_get_content, _set_content)

    def _get_size(self):
        size = len(self._get_content())
        if size != self.entry.st_size:
            self.entry.st_size = size
        return size

    size = property(_get_size)
