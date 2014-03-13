import threading


class FileChannel(object):
    #This channel lets different threads communicate by polling

    #Polling thread continuously calls read, data returned is either
    #from another thread calling write or None. It handles both cases.

    #Another thread calls write which blocks the channel until it is
    #read. 
    def __init__(self):
        self.lock = threading.Lock()
        self.data = None

    def write(self, data, file):
        self.lock.acquire()
        self.data = (data, file)

    def read(self):
        data = self.data
        if data:
            self.data = None
            self.lock.release()
        return data

file_channel = FileChannel()
