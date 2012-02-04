import collections
import errno
import functools
import select
import socket

'''Asynchronous socket service inspired by the basic design of Boost ASIO.

This service currently supports TCP sockets only, and supports asynchronous
versions of common client operations (connect, read, write) and server
operations (accept).

This implementation supports the use of select, poll, epoll, or kqueue as the
underlying poll system call.

Aaron Riekenberg
aaron.riekenberg@gmail.com
'''

ACCEPT_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EAGAIN, errno.EWOULDBLOCK])
CONNECT_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EINPROGRESS])
READ_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EAGAIN, errno.EWOULDBLOCK])
WRITE_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EAGAIN, errno.EWOULDBLOCK])
SOCKET_CLOSED_ERRNO = errno.EBADF

class AsyncException(Exception):

  def __init__(self, value):
    super().__init__()
    self.__value = value

  def __str__(self):
    return repr(self.__value)

class AsyncSocket(object):

  '''Socket class supporting asynchronous operations.'''

  class AcceptOperation(object):

    def __init__(self, asyncSocket, callback):
      super().__init__()
      self.__asyncSocket = asyncSocket
      self.__asyncIOService = asyncSocket.getAsyncIOService()
      self.__callback = callback
      self.__complete = False

    def isComplete(self):
      return self.__complete

    def poll(self):
      if self.__complete:
        return

      try:
        (newSocket, addr) = self.__asyncSocket.getSocket().accept()
        asyncSocket = AsyncSocket(self.__asyncIOService, newSocket)
        self.__setComplete(asyncSocket, 0)
      except socket.error as e:
        if e.errno in ACCEPT_WOULD_BLOCK_ERRNO_SET:
          self.__asyncIOService.registerAsyncSocketForRead(self.__asyncSocket)
        else:
          self.__setComplete(None, e.errno)

    def __setComplete(self, asyncSocket, error):
      if not self.__complete:
        self.__complete = True
        self.__asyncIOService.unregisterAsyncSocketForRead(self.__asyncSocket)
        self.__asyncIOService.invokeLater(
          functools.partial(self.__callback, asyncSocket = asyncSocket, error = error))

    def handleSocketError(self, error):
      self.__setComplete(asyncSocket = None, error = error)

  class ConnectOperation(object):

    def __init__(self, address, asyncSocket, callback):
      super().__init__()
      self.__address = address
      self.__asyncSocket = asyncSocket
      self.__asyncIOService = asyncSocket.getAsyncIOService()
      self.__callback = callback
      self.__complete = False
      self.__calledConnect = False

    def isComplete(self):
      return self.__complete

    def poll(self):
      if self.__complete:
        return

      if not self.__calledConnect:
        error = self.__asyncSocket.getSocket().connect_ex(self.__address)
        self.__calledConnect = True
        if error in CONNECT_WOULD_BLOCK_ERRNO_SET:
          self.__asyncIOService.registerAsyncSocketForWrite(self.__asyncSocket)
        else:
          self.__setComplete(error)
      else:
        error = self.__asyncSocket.getSocket().getsockopt(
                socket.SOL_SOCKET, socket.SO_ERROR)
        if error not in CONNECT_WOULD_BLOCK_ERRNO_SET:
          self.__setComplete(error)

    def __setComplete(self, error):
      if not self.__complete:
        self.__complete = True
        self.__asyncIOService.unregisterAsyncSocketForWrite(self.__asyncSocket)
        self.__asyncIOService.invokeLater(
          functools.partial(self.__callback, error = error))

    def handleSocketError(self, error):
      self.__setComplete(error)

  class ReadOperation(object):

    def __init__(self, maxBytes, asyncSocket, callback):
      super().__init__()
      self.__maxBytes = maxBytes
      self.__asyncSocket = asyncSocket
      self.__asyncIOService = asyncSocket.getAsyncIOService()
      self.__callback = callback
      self.__complete = False

    def isComplete(self):
      return self.__complete

    def poll(self):
      if self.__complete:
        return

      try:
        data = self.__asyncSocket.getSocket().recv(self.__maxBytes)
        self.__setComplete(data, 0)
      except socket.error as e:
        if e.errno in READ_WOULD_BLOCK_ERRNO_SET:
          self.__asyncIOService.registerAsyncSocketForRead(self.__asyncSocket)
        else:
          self.__setComplete(None, e.errno)

    def __setComplete(self, data, error):
      if not self.__complete:
        self.__complete = True
        self.__asyncIOService.unregisterAsyncSocketForRead(self.__asyncSocket)
        self.__asyncIOService.invokeLater(
          functools.partial(self.__callback, data = data, error = error))

    def handleSocketError(self, error):
      self.__setComplete(data = None, error = error)

  class WriteAllOperation(object):

    def __init__(self, writeBuffer, asyncSocket, callback):
      super().__init__()
      self.__writeBuffer = writeBuffer
      self.__asyncSocket = asyncSocket
      self.__asyncIOService = asyncSocket.getAsyncIOService()
      self.__callback = callback
      self.__complete = False

    def isComplete(self):
      return self.__complete

    def poll(self):
      if self.__complete:
        return

      writeWouldBlock = False
      try:
        bytesSent = self.__asyncSocket.getSocket().send(self.__writeBuffer)
        self.__writeBuffer = self.__writeBuffer[bytesSent:]
        if (len(self.__writeBuffer) == 0):
          self.__setComplete(0)
        else:
          writeWouldBlock = True
      except socket.error as e:
        if e.errno in WRITE_WOULD_BLOCK_ERRNO_SET:
          writeWouldBlock = True
        else:
          self.__setComplete(e.errno)

      if (writeWouldBlock):
        self.__asyncIOService.registerAsyncSocketForWrite(self.__asyncSocket)

    def __setComplete(self, error):
      if not self.__complete:
        self.__complete = True
        self.__asyncIOService.unregisterAsyncSocketForWrite(self.__asyncSocket)
        self.__asyncIOService.invokeLater(
          functools.partial(self.__callback, error = error))

    def handleSocketError(self, error):
      self.__setComplete(error)

  def __init__(self, asyncIOService, sock = None):
    super().__init__()
    self.__asyncIOService = asyncIOService
    self.__acceptOperation = None
    self.__connectOperation = None
    self.__readOperation = None
    self.__writeOperation = None
    self.__closed = False
    if sock:
      self.__socket = sock
    else:
      self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.__socket.setblocking(0)
    asyncIOService.addAsyncSocket(self)

  def __str__(self):
    return ('AsyncSocket [ fileno = {0} ]'.format(self.fileno()))

  def getsockname(self):
    return self.__socket.getsockname()

  def getpeername(self):
    return self.__socket.getpeername()
 
  def closed(self):
    return self.__closed

  def getAsyncIOService(self):
    return self.__asyncIOService

  def getSocket(self):
    return self.__socket

  def fileno(self):
    if self.__closed:
      return -1
    else:
      return self.__socket.fileno()

  def setReuseAddress(self):
    self.__socket.setsockopt(
      socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

  def listen(self, backlog = socket.SOMAXCONN):
    self.__socket.listen(backlog)

  def bind(self, addr):
    self.__socket.bind(addr)

  def asyncConnect(self, address, callback):
    if (self.__acceptOperation):
      raise AsyncException('Accept already in progress')
    if (self.__connectOperation):
      raise AsyncException('Connect already in progress')
    if (self.__readOperation):
      raise AsyncException('Read already in progress')
    if (self.__writeOperation):
      raise AsyncException('Write all already in progress')
    if (self.__closed):
      raise AsyncException('AsyncSocket closed')

    self.__connectOperation = self.__pollOperation(
      AsyncSocket.ConnectOperation(
        address = address,
        asyncSocket = self,
        callback = callback))

  def asyncAccept(self, callback):
    if (self.__acceptOperation):
      raise AsyncException('Accept already in progress')
    if (self.__connectOperation):
      raise AsyncException('Connect already in progress')
    if (self.__readOperation):
      raise AsyncException('Read already in progress')
    if (self.__writeOperation):
      raise AsyncException('Write all already in progress')
    if (self.__closed):
      raise AsyncException('AsyncSocket closed')

    self.__acceptOperation = self.__pollOperation(
      AsyncSocket.AcceptOperation(
        asyncSocket = self,
        callback = callback))

  def asyncRead(self, maxBytes, callback):
    if (self.__acceptOperation):
      raise AsyncException('Accept already in progress')
    if (self.__connectOperation):
      raise AsyncException('Connect already in progress')
    if (self.__readOperation):
      raise AsyncException('Read already in progress')
    if (self.__closed):
      raise AsyncException('AsyncSocket closed')

    self.__readOperation = self.__pollOperation(
      AsyncSocket.ReadOperation(
        maxBytes = maxBytes,
        asyncSocket = self,
        callback = callback))

  def asyncWriteAll(self, writeBuffer, callback):
    if (self.__acceptOperation):
      raise AsyncException('Accept already in progress')
    if (self.__connectOperation):
      raise AsyncException('Connect already in progress')
    if (self.__writeOperation):
      raise AsyncException('Write all already in progress')
    if (self.__closed):
      raise AsyncException('AsyncSocket closed')

    self.__writeOperation = self.__pollOperation(
      AsyncSocket.WriteAllOperation(
        writeBuffer = writeBuffer,
        asyncSocket = self,
        callback = callback))

  def close(self):
    if self.__closed:
      return

    self.__asyncIOService.removeAsyncSocket(self)
    self.__socket.close()
    self.__closed = True

    error = SOCKET_CLOSED_ERRNO

    self.__acceptOperation = self.__sendErrorToOperation(
      self.__acceptOperation, error)
    self.__connectOperation = self.__sendErrorToOperation(
      self.__connectOperation, error)
    self.__readOperation = self.__sendErrorToOperation(
      self.__readOperation, error)
    self.__writeOperation = self.__sendErrorToOperation(
      self.__writeOperation, error)

  def handleError(self):
    error = self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    self.__acceptOperation = self.__sendErrorToOperation(
      self.__acceptOperation, error)
    self.__connectOperation = self.__sendErrorToOperation(
      self.__connectOperation, error)
    self.__readOperation = self.__sendErrorToOperation(
      self.__readOperation, error)
    self.__writeOperation = self.__sendErrorToOperation(
      self.__writeOperation, error)

  def handleRead(self):
    self.__acceptOperation = self.__pollOperation(self.__acceptOperation)
    self.__readOperation = self.__pollOperation(self.__readOperation)

  def handleWrite(self):
    self.__connectOperation = self.__pollOperation(self.__connectOperation)
    self.__writeOperation = self.__pollOperation(self.__writeOperation)

  def __pollOperation(self, operation):
    if operation:
      operation.poll()
      if operation.isComplete():
        operation = None
    return operation

  def __sendErrorToOperation(self, operation, error):
    if operation:
      operation.handleSocketError(error)
      if operation.isComplete():
        operation = None
    return operation

class AsyncIOService(object):

  '''Service used to poll asynchronous sockets.'''

  def __init__(self):
    super().__init__()
    self.__fdToAsyncSocket = {}
    self.__fdsRegisteredForRead = set()
    self.__fdsRegisteredForWrite = set()
    self.__eventQueue = collections.deque()

  def createAsyncSocket(self):
    return AsyncSocket(asyncIOService = self)

  def addAsyncSocket(self, asyncSocket):
    self.__fdToAsyncSocket[asyncSocket.fileno()] = asyncSocket

  def removeAsyncSocket(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno in self.__fdToAsyncSocket:
      del self.__fdToAsyncSocket[fileno]
    if ((fileno in self.__fdsRegisteredForRead) or
        (fileno in self.__fdsRegisteredForWrite)):
      self.unregisterForEvents(asyncSocket)
      self.__fdsRegisteredForRead.discard(fileno)
      self.__fdsRegisteredForWrite.discard(fileno)

  def invokeLater(self, event):
    self.__eventQueue.append(event)

  def registerAsyncSocketForRead(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno not in self.__fdsRegisteredForRead:
      if fileno in self.__fdsRegisteredForWrite:
        self.modifyRegistrationForEvents(asyncSocket, readEvents = True, writeEvents = True)
      else:
        self.registerForEvents(asyncSocket, readEvents = True, writeEvents = False)
      self.__fdsRegisteredForRead.add(fileno)

  def unregisterAsyncSocketForRead(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno in self.__fdsRegisteredForRead:
      if fileno in self.__fdsRegisteredForWrite:
        self.modifyRegistrationForEvents(asyncSocket, readEvents = False, writeEvents = True)
      else:
        self.unregisterForEvents(asyncSocket)
      self.__fdsRegisteredForRead.discard(fileno)

  def registerAsyncSocketForWrite(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno not in self.__fdsRegisteredForWrite:
      if fileno in self.__fdsRegisteredForRead:
        self.modifyRegistrationForEvents(asyncSocket, readEvents = True, writeEvents = True)
      else:
        self.registerForEvents(asyncSocket, readEvents = False, writeEvents = True)
      self.__fdsRegisteredForWrite.add(fileno)

  def unregisterAsyncSocketForWrite(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno in self.__fdsRegisteredForWrite:
      if fileno in self.__fdsRegisteredForRead:
        self.modifyRegistrationForEvents(asyncSocket, readEvents = True, writeEvents = False)
      else:
        self.unregisterForEvents(asyncSocket)
      self.__fdsRegisteredForWrite.discard(fileno)

  def getReadFDSet(self):
    return self.__fdsRegisteredForRead

  def getWriteFDSet(self):
    return self.__fdsRegisteredForWrite

  def getNumFDs(self):
    return len(self.__fdToAsyncSocket)

  def registerForEvents(self, asyncSocket, readEvents, writeEvents):
    raise NotImplementedError

  def modifyRegistrationForEvents(self, asyncSocket, readEvents, writeEvents):
    raise NotImplementedError

  def unregisterForEvents(self, asyncSocket):
    raise NotImplementedError

  def doPoll(self, block):
    raise NotImplementedError

  def run(self):
    while True:
      # As we process events in self.__eventQueue, more events are likely
      # to be added to it by invokeLater.  We don't want to starve events
      # coming in from doPoll, so we limit the number of events processed
      # from self.__eventQueue to the initial size of the queue.  After this if
      # the queue is still not empty, set doPoll to be non blocking so we get
      # back to processing events in the queue in a timely manner.
      initialQueueLength = len(self.__eventQueue)
      eventsProcessed = 0
      while ((len(self.__eventQueue) > 0) and
             (eventsProcessed < initialQueueLength)):
        event = self.__eventQueue.popleft()
        event()
        eventsProcessed += 1

      if ((len(self.__eventQueue) == 0) and
          (len(self.__fdsRegisteredForRead) == 0) and
          (len(self.__fdsRegisteredForWrite) == 0)):
        break

      block = True
      if (len(self.__eventQueue) > 0):
        block = False
      self.doPoll(block = block)

  def handleEventForFD(self, fd, readReady, writeReady, errorReady):
    if fd in self.__fdToAsyncSocket:
      asyncSocket = self.__fdToAsyncSocket[fd]
      if (readReady):
        asyncSocket.handleRead()
      if (writeReady):
        asyncSocket.handleWrite()
      if (errorReady):
        asyncSocket.handleError()

class EPollAsyncIOService(AsyncIOService):

  def __init__(self):
    super().__init__()
    self.__poller = select.epoll()

  def __str__(self):
    return ('EPollAsyncIOService [ fileno = {0} ]'.format(self.__poller.fileno()))

  def registerForEvents(self, asyncSocket, readEvents, writeEvents):
    fileno = asyncSocket.fileno()
    eventMask = 0
    if (readEvents):
      eventMask |= select.EPOLLIN
    if (writeEvents):
      eventMask |= select.EPOLLOUT
    self.__poller.register(fileno, eventMask)

  def modifyRegistrationForEvents(self, asyncSocket, readEvents, writeEvents):
    fileno = asyncSocket.fileno()
    eventMask = 0
    if (readEvents):
      eventMask |= select.EPOLLIN
    if (writeEvents):
      eventMask |= select.EPOLLOUT
    self.__poller.modify(fileno, eventMask)

  def unregisterForEvents(self, asyncSocket):
    fileno = asyncSocket.fileno()
    self.__poller.unregister(fileno)

  def doPoll(self, block):
    readyList = self.__poller.poll(-1 if block else 0)
    for (fd, eventMask) in readyList:
      readReady = ((eventMask & select.EPOLLIN) != 0)
      writeReady = ((eventMask & select.EPOLLOUT) != 0)
      errorReady = ((eventMask & 
                     (select.EPOLLERR | select.EPOLLHUP)) != 0)
      self.handleEventForFD(fd = fd,
                            readReady = readReady,
                            writeReady = writeReady,
                            errorReady = errorReady)

class KQueueAsyncIOService(AsyncIOService):

  def __init__(self):
    super().__init__()
    self.__kqueue = select.kqueue()

  def __str__(self):
    return ('KQueueAsyncIOService [ fileno = {0} ]'.format(self.__kqueue.fileno()))

  def registerForEvents(self, asyncSocket, readEvents, writeEvents):
    fileno = asyncSocket.fileno()
    if readEvents:
      readKE = select.kevent(ident = fileno,
                             filter = select.KQ_FILTER_READ,
                             flags = select.KQ_EV_ADD)
    else:
      readKE = select.kevent(ident = fileno,
                             filter = select.KQ_FILTER_READ,
                             flags = (select.KQ_EV_ADD | select.KQ_EV_DISABLE))
    if writeEvents:
      writeKE = select.kevent(ident = fileno,
                              filter = select.KQ_FILTER_WRITE,
                              flags = select.KQ_EV_ADD)
    else:
      writeKE = select.kevent(ident = fileno,
                              filter = select.KQ_FILTER_WRITE,
                              flags = (select.KQ_EV_ADD | select.KQ_EV_DISABLE))
    # Should be able to put readKE and writeKE in a list in
    # one call to kqueue.control, but this is broken due to Python issue 5910
    self.__kqueue.control([readKE], 0, 0)
    self.__kqueue.control([writeKE], 0, 0)

  def modifyRegistrationForEvents(self, asyncSocket, readEvents, writeEvents):
    fileno = asyncSocket.fileno()
    if readEvents:
      readKE = select.kevent(ident = fileno,
                             filter = select.KQ_FILTER_READ,
                             flags = select.KQ_EV_ENABLE)
    else:
      readKE = select.kevent(ident = fileno,
                             filter = select.KQ_FILTER_READ,
                             flags = select.KQ_EV_DISABLE)
    if writeEvents:
      writeKE = select.kevent(ident = fileno,
                              filter = select.KQ_FILTER_WRITE,
                              flags = select.KQ_EV_ENABLE)
    else:
      writeKE = select.kevent(ident = fileno,
                              filter = select.KQ_FILTER_WRITE,
                              flags = select.KQ_EV_DISABLE)
    # Should be able to put readKE and writeKE in a list in
    # one call to kqueue.control, but this is broken due to Python issue 5910
    self.__kqueue.control([readKE], 0, 0)
    self.__kqueue.control([writeKE], 0, 0)

  def unregisterForEvents(self, asyncSocket):
    fileno = asyncSocket.fileno()
    readKE = select.kevent(ident = fileno,
                           filter = select.KQ_FILTER_READ,
                           flags = select.KQ_EV_DELETE)
    writeKE = select.kevent(ident = fileno,
                            filter = select.KQ_FILTER_WRITE,
                            flags = select.KQ_EV_DELETE)
    # Should be able to put readKE and writeKE in a list in
    # one call to kqueue.control, but this is broken due to Python issue 5910
    self.__kqueue.control([readKE], 0, 0)
    self.__kqueue.control([writeKE], 0, 0)

  def doPoll(self, block):
    eventList = self.__kqueue.control(
                  None,
                  self.getNumFDs() * 2,
                  None if block else 0)
    for ke in eventList:
      fd = ke.ident
      readReady = (ke.filter == select.KQ_FILTER_READ)
      writeReady = (ke.filter == select.KQ_FILTER_WRITE)
      errorReady = ((ke.flags & select.KQ_EV_EOF) != 0)
      self.handleEventForFD(fd = fd,
                            readReady = readReady,
                            writeReady = writeReady,
                            errorReady = errorReady)

class PollAsyncIOService(AsyncIOService):

  def __init__(self):
    super().__init__()
    self.__poller = select.poll()

  def __str__(self):
    return 'PollAsyncIOService'

  def registerForEvents(self, asyncSocket, readEvents, writeEvents):
    fileno = asyncSocket.fileno()
    eventMask = 0
    if (readEvents):
      eventMask |= select.POLLIN
    if (writeEvents):
      eventMask |= select.POLLOUT
    self.__poller.register(fileno, eventMask)

  def modifyRegistrationForEvents(self, asyncSocket, readEvents, writeEvents):
    fileno = asyncSocket.fileno()
    eventMask = 0
    if (readEvents):
      eventMask |= select.POLLIN
    if (writeEvents):
      eventMask |= select.POLLOUT
    self.__poller.modify(fileno, eventMask)

  def unregisterForEvents(self, asyncSocket):
    fileno = asyncSocket.fileno()
    self.__poller.unregister(fileno)

  def doPoll(self, block):
    readyList = self.__poller.poll(None if block else 0)
    for (fd, eventMask) in readyList:
      readReady = ((eventMask & select.POLLIN) != 0)
      writeReady = ((eventMask & select.POLLOUT) != 0)
      errorReady = ((eventMask & 
                     (select.POLLERR | select.POLLHUP | select.POLLNVAL)) != 0)
      self.handleEventForFD(fd = fd,
                            readReady = readReady,
                            writeReady = writeReady,
                            errorReady = errorReady)

class SelectAsyncIOService(AsyncIOService):

  def __init__(self):
    super().__init__()

  def __str__(self):
    return 'SelectAsyncIOService'

  def registerForEvents(self, asyncSocket, readEvents, writeEvents):
    pass

  def modifyRegistrationForEvents(self, asyncSocket, readEvents, writeEvents):
    pass

  def unregisterForEvents(self, asyncSocket):
    pass

  def doPoll(self, block):
    allFDSet = self.getReadFDSet() | self.getWriteFDSet()
    (readList, writeList, exceptList) = \
      select.select(self.getReadFDSet(), self.getWriteFDSet(), allFDSet,
                    None if block else 0)
    for fd in allFDSet:
      readReady = fd in readList
      writeReady = fd in writeList
      errorReady = fd in exceptList
      if (readReady or writeReady or errorReady):
        self.handleEventForFD(fd = fd,
                              readReady = readReady,
                              writeReady = writeReady,
                              errorReady = errorReady)

def createAsyncIOService(allow_epoll = True,
                         allow_kqueue = True,
                         allow_poll = True):
  '''Create an AsyncIOService supported by the platform and parameters.'''

  if (allow_epoll and hasattr(select, 'epoll')):
    return EPollAsyncIOService()
  elif (allow_kqueue and hasattr(select, 'kqueue')):
    return KQueueAsyncIOService()
  elif (allow_poll and hasattr(select, 'poll')):
    return PollAsyncIOService()
  else:
    return SelectAsyncIOService()
