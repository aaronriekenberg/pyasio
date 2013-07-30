# Copyright (C) 2012 Aaron Riekenberg (aaron.riekenberg@gmail.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in 
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import abc
import collections
import errno
import functools
import heapq
import itertools
import os
import select
import socket
import sys
import time

'''Asynchronous socket service inspired by the basic design of Boost ASIO.

This service currently supports TCP sockets only, and supports asynchronous
versions of common client operations (connect, read, write) and server
operations (accept).

This implementation supports the use of select, poll, epoll, or kqueue as the
underlying poll system call.

Aaron Riekenberg
aaron.riekenberg@gmail.com
'''

def _checkPythonVersion():
  if ((sys.version_info.major < 3) or (sys.version_info.minor < 3)):
    raise ImportError('Python 3.3 or newer required')

_checkPythonVersion()

_ACCEPT_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EAGAIN, errno.EWOULDBLOCK])
_CONNECT_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EINPROGRESS, errno.EINTR])
_READ_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EAGAIN, errno.EWOULDBLOCK])
_WRITE_WOULD_BLOCK_ERRNO_SET = frozenset([errno.EAGAIN, errno.EWOULDBLOCK])
_INTERRUPTED_ERRNO = errno.EINTR
_SOCKET_CLOSED_ERRNO = errno.EBADF
_NO_ERROR_ERRNO = 0

def _signalSafe(function):
  def wrapper(*args, **kwargs):
    while True:
      try:
        return function(*args, **kwargs)
      except OSError as e:
        if (e.errno == _INTERRUPTED_ERRNO):
          pass
        else:
          raise
  return wrapper

class AsyncException(Exception):

  def __init__(self, value):
    super().__init__()
    self.__value = value

  def __str__(self):
    return repr(self.__value)

class ErrorObject:

  def __init__(self, errnoValue):
    super().__init__()
    self.__errnoValue = errnoValue
    self.__stringValue = None

  def __bool__(self):
    return (self.__errnoValue != _NO_ERROR_ERRNO)

  def __str__(self):
    if (self.__stringValue is None):
      self.__stringValue = '{} (errno={})'.format(
        os.strerror(self.__errnoValue), self.__errnoValue)
    return self.__stringValue

_NO_ERROR_OBJECT = ErrorObject(errnoValue = _NO_ERROR_ERRNO)

class _AbstractAsyncOperation(metaclass = abc.ABCMeta):

  @abc.abstractmethod
  def _isComplete(self):
    raise NotImplementedError

  @abc.abstractmethod
  def _poll(self):
    raise NotImplementedError

  @abc.abstractmethod
  def _handleSocketError(self, errorObject):
    raise NotImplementedError

class _AsyncAcceptOperation(_AbstractAsyncOperation):

  def __init__(self, asyncSocket, callback):
    super().__init__()
    self.__asyncSocket = asyncSocket
    self.__asyncIOService = asyncSocket.getAsyncIOService()
    self.__callback = callback
    self.__complete = False

  def _isComplete(self):
    return self.__complete

  @_signalSafe
  def __signalSafeAccept(self):
    return self.__asyncSocket.getSocket().accept()

  def _poll(self):
    if self.__complete:
      return

    try:
      (newSocket, addr) = self.__signalSafeAccept()
      asyncSocket = AsyncSocket(self.__asyncIOService, newSocket)
      self.__setComplete(asyncSocket, _NO_ERROR_OBJECT)
    except OSError as e:
      if e.errno in _ACCEPT_WOULD_BLOCK_ERRNO_SET:
        self.__asyncIOService._registerAsyncSocketForRead(self.__asyncSocket)
      else:
        self.__setComplete(None, ErrorObject(errnoValue = e.errno))

  def __setComplete(self, asyncSocket, errorObject):
    if not self.__complete:
      self.__complete = True
      self.__asyncIOService._unregisterAsyncSocketForRead(self.__asyncSocket)
      self.__asyncIOService._invokeLater(
        functools.partial(self.__callback, asyncSocket = asyncSocket,
                          error = errorObject))

  def _handleSocketError(self, errorObject):
    self.__setComplete(asyncSocket = None, errorObject = errorObject)

class _AsyncConnectOperation(_AbstractAsyncOperation):

  def __init__(self, address, asyncSocket, callback):
    super().__init__()
    self.__address = address
    self.__asyncSocket = asyncSocket
    self.__asyncIOService = asyncSocket.getAsyncIOService()
    self.__callback = callback
    self.__complete = False
    self.__calledConnect = False

  def _isComplete(self):
    return self.__complete

  def _poll(self):
    if self.__complete:
      return

    if not self.__calledConnect:
      errnoValue = self.__asyncSocket.getSocket().connect_ex(self.__address)
      self.__calledConnect = True
      if errnoValue in _CONNECT_WOULD_BLOCK_ERRNO_SET:
        self.__asyncIOService._registerAsyncSocketForWrite(self.__asyncSocket)
      else:
        self.__setComplete(ErrorObject(errnoValue = errnoValue))
    else:
      errnoValue = self.__asyncSocket.getSocket().getsockopt(
                     socket.SOL_SOCKET, socket.SO_ERROR)
      if errnoValue not in _CONNECT_WOULD_BLOCK_ERRNO_SET:
        self.__setComplete(ErrorObject(errnoValue = errnoValue))

  def __setComplete(self, errorObject):
    if not self.__complete:
      self.__complete = True
      self.__asyncIOService._unregisterAsyncSocketForWrite(self.__asyncSocket)
      self.__asyncIOService._invokeLater(
        functools.partial(self.__callback, error = errorObject))

  def _handleSocketError(self, errorObject):
    self.__setComplete(errorObject)

class _AsyncReadOperation(_AbstractAsyncOperation):

  def __init__(self, maxBytes, asyncSocket, callback):
    super().__init__()
    self.__maxBytes = maxBytes
    self.__asyncSocket = asyncSocket
    self.__asyncIOService = asyncSocket.getAsyncIOService()
    self.__callback = callback
    self.__complete = False

  def _isComplete(self):
    return self.__complete

  @_signalSafe
  def __signalSafeRecv(self):
    return self.__asyncSocket.getSocket().recv(self.__maxBytes)

  def _poll(self):
    if self.__complete:
      return

    try:
      data = self.__signalSafeRecv()
      self.__setComplete(data, _NO_ERROR_OBJECT)
    except OSError as e:
      if e.errno in _READ_WOULD_BLOCK_ERRNO_SET:
        self.__asyncIOService._registerAsyncSocketForRead(self.__asyncSocket)
      else:
        self.__setComplete(None, ErrorObject(errnoValue = e.errno))

  def __setComplete(self, data, errorObject):
    if not self.__complete:
      self.__complete = True
      self.__asyncIOService._unregisterAsyncSocketForRead(self.__asyncSocket)
      self.__asyncIOService._invokeLater(
        functools.partial(self.__callback, data = data, error = errorObject))

  def _handleSocketError(self, errorObject):
    self.__setComplete(data = None, errorObject = errorObject)

class _AsyncWriteAllOperation(_AbstractAsyncOperation):

  def __init__(self, writeBuffer, asyncSocket, callback):
    super().__init__()
    self.__writeBuffer = writeBuffer
    self.__asyncSocket = asyncSocket
    self.__asyncIOService = asyncSocket.getAsyncIOService()
    self.__callback = callback
    self.__complete = False

  def _isComplete(self):
    return self.__complete

  @_signalSafe
  def __signalSafeSend(self):
    return self.__asyncSocket.getSocket().send(self.__writeBuffer)

  def _poll(self):
    if self.__complete:
      return

    writeWouldBlock = False
    try:
      bytesSent = self.__signalSafeSend()
      self.__writeBuffer = self.__writeBuffer[bytesSent:]
      if (len(self.__writeBuffer) == 0):
        self.__setComplete(_NO_ERROR_OBJECT)
      else:
        writeWouldBlock = True
    except OSError as e:
      if e.errno in _WRITE_WOULD_BLOCK_ERRNO_SET:
        writeWouldBlock = True
      else:
        self.__setComplete(ErrorObject(errnoValue = e.errno))

    if (writeWouldBlock):
      self.__asyncIOService._registerAsyncSocketForWrite(self.__asyncSocket)

  def __setComplete(self, errorObject):
    if not self.__complete:
      self.__complete = True
      self.__asyncIOService._unregisterAsyncSocketForWrite(self.__asyncSocket)
      self.__asyncIOService._invokeLater(
        functools.partial(self.__callback, error = errorObject))

  def _handleSocketError(self, errorObject):
    self.__setComplete(errorObject)

class AsyncSocket:

  '''Socket class supporting asynchronous operations.'''

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
    asyncIOService._addAsyncSocket(self)

  def __str__(self):
    return ('AsyncSocket [ fileno = {} ]'.format(self.fileno()))

  def getsockname(self):
    self.assertNotClosed()
    return self.__socket.getsockname()

  def getpeername(self):
    self.assertNotClosed()
    return self.__socket.getpeername()
 
  def closed(self):
    return self.__closed

  def assertNotClosed(self):
    if self.__closed:
      raise AsyncException('AsyncSocket closed')

  def getAsyncIOService(self):
    return self.__asyncIOService

  def getSocket(self):
    return self.__socket

  def fileno(self):
    self.assertNotClosed()
    return self.__socket.fileno()

  def setReuseAddress(self):
    self.assertNotClosed()
    self.__socket.setsockopt(
      socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

  def listen(self, backlog = socket.SOMAXCONN):
    self.assertNotClosed()
    self.__socket.listen(backlog)

  def bind(self, addr):
    self.assertNotClosed()
    self.__socket.bind(addr)

  def __verifyStateForNewOperation(
    self, ignoreInProgressRead = False, ignoreInProgressWrite = False):
    if (self.__closed):
      raise AsyncException('AsyncSocket closed')
    if (self.__acceptOperation):
      raise AsyncException('Accept already in progress')
    if (self.__connectOperation):
      raise AsyncException('Connect already in progress')
    if ((not ignoreInProgressRead) and self.__readOperation):
      raise AsyncException('Read already in progress')
    if ((not ignoreInProgressWrite) and self.__writeOperation):
      raise AsyncException('Write all already in progress')

  def asyncConnect(self, address, callback):
    self.__verifyStateForNewOperation()

    self.__connectOperation = self.__pollOperation(
      _AsyncConnectOperation(
        address = address,
        asyncSocket = self,
        callback = callback))

  def asyncAccept(self, callback):
    self.__verifyStateForNewOperation()

    self.__acceptOperation = self.__pollOperation(
      _AsyncAcceptOperation(
        asyncSocket = self,
        callback = callback))

  def asyncRead(self, maxBytes, callback):
    self.__verifyStateForNewOperation(ignoreInProgressWrite = True)

    self.__readOperation = self.__pollOperation(
      _AsyncReadOperation(
        maxBytes = maxBytes,
        asyncSocket = self,
        callback = callback))

  def asyncWriteAll(self, writeBuffer, callback):
    self.__verifyStateForNewOperation(ignoreInProgressRead = True)

    self.__writeOperation = self.__pollOperation(
      _AsyncWriteAllOperation(
        writeBuffer = writeBuffer,
        asyncSocket = self,
        callback = callback))

  @_signalSafe
  def __signalSafeClose(self):
    self.__socket.close()

  def close(self):
    if self.__closed:
      return

    errorObject = ErrorObject(errnoValue = _SOCKET_CLOSED_ERRNO)

    self.__acceptOperation = self.__sendErrorToOperation(
      self.__acceptOperation, errorObject)
    self.__connectOperation = self.__sendErrorToOperation(
      self.__connectOperation, errorObject)
    self.__readOperation = self.__sendErrorToOperation(
      self.__readOperation, errorObject)
    self.__writeOperation = self.__sendErrorToOperation(
      self.__writeOperation, errorObject)

    self.__asyncIOService._removeAsyncSocket(self)
    self.__signalSafeClose()
    self.__closed = True

  def _handleErrorReady(self):
    error = ErrorObject(
      errnoValue = self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR))
    if (error):
      self.__acceptOperation = self.__sendErrorToOperation(
        self.__acceptOperation, error)
      self.__connectOperation = self.__sendErrorToOperation(
        self.__connectOperation, error)
      self.__readOperation = self.__sendErrorToOperation(
        self.__readOperation, error)
      self.__writeOperation = self.__sendErrorToOperation(
        self.__writeOperation, error)

  def _handleReadReady(self):
    self.__acceptOperation = self.__pollOperation(self.__acceptOperation)
    self.__readOperation = self.__pollOperation(self.__readOperation)

  def _handleWriteReady(self):
    self.__connectOperation = self.__pollOperation(self.__connectOperation)
    self.__writeOperation = self.__pollOperation(self.__writeOperation)

  def __pollOperation(self, operation):
    if operation is not None:
      operation._poll()
      if operation._isComplete():
        operation = None
    return operation

  def __sendErrorToOperation(self, operation, errorObject):
    if operation is not None:
      operation._handleSocketError(errorObject)
      if operation._isComplete():
        operation = None
    return operation

class AsyncTimer:

  def __init__(self, deltaTimeSeconds, callback, asyncIOService):
    super().__init__()
    self.__absoluteTimeoutTimeSeconds = time.time() + deltaTimeSeconds
    self.__callback = callback
    self.__callbackFired = False
    self.__cancelled = False
    self.__asyncIOService = asyncIOService

  def _getAbsoluteTimeoutTimeSeconds(self):
    return self.__absoluteTimeoutTimeSeconds

  def _fireCallback(self):
    if not self.__callbackFired:
      self.__callbackFired = True
      self.__asyncIOService._invokeLater(self.__callback)

  def cancel(self):
    self.__cancelled = True

  def cancelled(self):
    return self.__cancelled

class _AsyncTimerService:

  def __init__(self):
    super().__init__()
    self.__asyncTimerHeap = []
    self.__heapCounter = itertools.count()

  def __cleanupCancelledTimers(self):
    # Instead of inefficiently removing non-minimal elements from the heap
    # when a timer is cancelled, we leave cancelled timers in the heap but
    # flag them as cancelled.
    # This method removes the earliest cancelled timers from the heap until
    # either a non-cancelled timer is found, or the heap is empty.
    while self.__asyncTimerHeap:
      asyncTimer = self.__asyncTimerHeap[0][2]
      if asyncTimer.cancelled():
        heapq.heappop(self.__asyncTimerHeap)
      else:
        break

  def __peekAtEarliestTimer(self):
    self.__cleanupCancelledTimers()

    if self.__asyncTimerHeap:
      return self.__asyncTimerHeap[0][2]
    return None

  def _scheduleTimer(self, asyncTimer):
    # 3-tuple idea borrowed from http://docs.python.org/3/library/heapq.html
    heapTuple = (asyncTimer._getAbsoluteTimeoutTimeSeconds(),
                 next(self.__heapCounter), asyncTimer)
    heapq.heappush(self.__asyncTimerHeap, heapTuple)

  def _getNumPendingTimers(self):
    self.__cleanupCancelledTimers()

    return len(self.__asyncTimerHeap)

  def _getEarliestTimeoutDeltaSeconds(self):
    earliestTimer = self.__peekAtEarliestTimer()
    if earliestTimer is None:
      return None
    else:
      return max(0,
                 earliestTimer._getAbsoluteTimeoutTimeSeconds() - time.time())

  def _firePendingTimers(self):
    while True:
      earliestTimer = self.__peekAtEarliestTimer()
      if earliestTimer is None:
        break
      elif (time.time() < earliestTimer._getAbsoluteTimeoutTimeSeconds()):
        break
      else:
        heapq.heappop(self.__asyncTimerHeap)
        earliestTimer._fireCallback()

class _AsyncFDService:

  def __init__(self, poller):
    super().__init__()
    self.__fdToAsyncSocket = {}
    self.__fdsRegisteredForRead = set()
    self.__fdsRegisteredForWrite = set()
    self.__poller = poller

  def _addAsyncSocket(self, asyncSocket):
    self.__fdToAsyncSocket[asyncSocket.fileno()] = asyncSocket

  def _removeAsyncSocket(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno in self.__fdToAsyncSocket:
      del self.__fdToAsyncSocket[fileno]
    if ((fileno in self.__fdsRegisteredForRead) or
        (fileno in self.__fdsRegisteredForWrite)):
      self.__poller._unregisterForEvents(fileno)
      self.__fdsRegisteredForRead.discard(fileno)
      self.__fdsRegisteredForWrite.discard(fileno)

  def _registerAsyncSocketForRead(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno not in self.__fdsRegisteredForRead:
      if fileno in self.__fdsRegisteredForWrite:
        self.__poller._modifyRegistrationForEvents(
          fileno, readEvents = True, writeEvents = True)
      else:
        self.__poller._registerForEvents(
          fileno, readEvents = True, writeEvents = False)
      self.__fdsRegisteredForRead.add(fileno)

  def _unregisterAsyncSocketForRead(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno in self.__fdsRegisteredForRead:
      if fileno in self.__fdsRegisteredForWrite:
        self.__poller._modifyRegistrationForEvents(
          fileno, readEvents = False, writeEvents = True)
      else:
        self.__poller._unregisterForEvents(fileno)
      self.__fdsRegisteredForRead.discard(fileno)

  def _registerAsyncSocketForWrite(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno not in self.__fdsRegisteredForWrite:
      if fileno in self.__fdsRegisteredForRead:
        self.__poller._modifyRegistrationForEvents(
          fileno, readEvents = True, writeEvents = True)
      else:
        self.__poller._registerForEvents(
          fileno, readEvents = False, writeEvents = True)
      self.__fdsRegisteredForWrite.add(fileno)

  def _unregisterAsyncSocketForWrite(self, asyncSocket):
    fileno = asyncSocket.fileno()
    if fileno in self.__fdsRegisteredForWrite:
      if fileno in self.__fdsRegisteredForRead:
        self.__poller._modifyRegistrationForEvents(
          fileno, readEvents = True, writeEvents = False)
      else:
        self.__poller._unregisterForEvents(fileno)
      self.__fdsRegisteredForWrite.discard(fileno)

  def _getNumFDsRegisteredForRead(self):
    return len(self.__fdsRegisteredForRead)

  def _getNumFDsRegisteredForWrite(self):
    return len(self.__fdsRegisteredForWrite)

  def _pollFDs(self, blockSeconds):
    self.__poller._poll(
      blockSeconds = blockSeconds,
      eventCallback = self.__handleEventForFD)

  def __handleEventForFD(self, fd, readReady, writeReady, errorReady):
    asyncSocket = self.__fdToAsyncSocket.get(fd)
    if asyncSocket is not None:
      if (readReady):
        asyncSocket._handleReadReady()
      if (writeReady):
        asyncSocket._handleWriteReady()
      if (errorReady):
        asyncSocket._handleErrorReady()

class AsyncIOService:

  '''Service used to poll asynchronous sockets.'''

  def __init__(self, poller):
    super().__init__()
    self.__poller = poller
    self.__eventQueue = collections.deque()
    self.__asyncTimerService = _AsyncTimerService()
    self.__asyncFDService = _AsyncFDService(poller = poller)

  def __str__(self):
    return 'AsyncIOService [ poller = ' + str(self.__poller) + ' ]'

  def createAsyncSocket(self):
    return AsyncSocket(asyncIOService = self)

  def scheduleTimer(self, deltaTimeSeconds, callback):
    asyncTimer = AsyncTimer(
                   deltaTimeSeconds = deltaTimeSeconds,
                   callback = callback,
                   asyncIOService = self)
    self.__asyncTimerService._scheduleTimer(asyncTimer)
    return asyncTimer

  def _invokeLater(self, event):
    self.__eventQueue.append(event)

  def _addAsyncSocket(self, asyncSocket):
    self.__asyncFDService._addAsyncSocket(asyncSocket)

  def _removeAsyncSocket(self, asyncSocket):
    self.__asyncFDService._removeAsyncSocket(asyncSocket)

  def _registerAsyncSocketForRead(self, asyncSocket):
    self.__asyncFDService._registerAsyncSocketForRead(asyncSocket)

  def _unregisterAsyncSocketForRead(self, asyncSocket):
    self.__asyncFDService._unregisterAsyncSocketForRead(asyncSocket)

  def _registerAsyncSocketForWrite(self, asyncSocket):
    self.__asyncFDService._registerAsyncSocketForWrite(asyncSocket)

  def _unregisterAsyncSocketForWrite(self, asyncSocket):
    self.__asyncFDService._unregisterAsyncSocketForWrite(asyncSocket)

  def run(self):

    def computeBlockSeconds():
      blockSeconds = 0
      if not self.__eventQueue:
        # self.__eventQueue is empty, so block for one minute or until the
        # next timer pop, whichever comes first.
        blockSeconds = self.__asyncTimerService._getEarliestTimeoutDeltaSeconds()
        if ((blockSeconds is None) or (blockSeconds > 60)):
          blockSeconds = 60
      return blockSeconds

    while True:
      # As we process events in self.__eventQueue, more events are likely
      # to be added to it by _invokeLater.  We don't want to starve events
      # coming in from _poll, so we limit the number of events processed
      # from self.__eventQueue to the initial size of the queue.  After this if
      # the queue is still not empty, set block seconds to 0 so we get
      # back to processing events in the queue in a timely manner.
      initialQueueLength = len(self.__eventQueue)
      eventsProcessed = 0
      while (self.__eventQueue and
             (eventsProcessed < initialQueueLength)):
        event = self.__eventQueue.popleft()
        event()
        eventsProcessed += 1
      # Set event to None so we don't hang on to a reference to event during the
      # potentially long-running _pollFDs below.
      event = None

      if ((not self.__eventQueue) and
          (self.__asyncFDService._getNumFDsRegisteredForRead() == 0) and
          (self.__asyncFDService._getNumFDsRegisteredForWrite() == 0) and
          (self.__asyncTimerService._getNumPendingTimers() == 0)):
        # The event queue is empty, and there are no events to wait for.
        # Break out of the loop.
        break

      self.__asyncFDService._pollFDs(blockSeconds = computeBlockSeconds())

      self.__asyncTimerService._firePendingTimers()

class _AbstractPoller(metaclass = abc.ABCMeta):

  @staticmethod
  @abc.abstractmethod
  def _isAvailable():
    raise NotImplementedError

  @abc.abstractmethod
  def _registerForEvents(self, fileno, readEvents, writeEvents):
    raise NotImplementedError

  @abc.abstractmethod
  def _modifyRegistrationForEvents(self, fileno, readEvents, writeEvents):
    raise NotImplementedError

  @abc.abstractmethod
  def _unregisterForEvents(self, fileno):
    raise NotImplementedError

  @abc.abstractmethod
  def _poll(self, blockSeconds, eventCallback):
    raise NotImplementedError

class _EPollPoller(_AbstractPoller):

  @staticmethod
  def _isAvailable():
    return hasattr(select, 'epoll')

  def __init__(self):
    super().__init__()
    self.__poller = select.epoll()

  def __str__(self):
    return ('EPollPoller [ fileno = {} ]'.format(self.__poller.fileno()))

  def _registerForEvents(self, fileno, readEvents, writeEvents):
    eventMask = 0
    if (readEvents):
      eventMask |= select.EPOLLIN
    if (writeEvents):
      eventMask |= select.EPOLLOUT
    self.__poller.register(fileno, eventMask)

  def _modifyRegistrationForEvents(self, fileno, readEvents, writeEvents):
    eventMask = 0
    if (readEvents):
      eventMask |= select.EPOLLIN
    if (writeEvents):
      eventMask |= select.EPOLLOUT
    self.__poller.modify(fileno, eventMask)

  def _unregisterForEvents(self, fileno):
    self.__poller.unregister(fileno)

  @_signalSafe
  def __poll(self, blockSeconds):
    return self.__poller.poll(blockSeconds)

  def _poll(self, blockSeconds, eventCallback):
    readyList = self.__poll(blockSeconds)
    for (fd, eventMask) in readyList:
      readReady = ((eventMask & select.EPOLLIN) != 0)
      writeReady = ((eventMask & select.EPOLLOUT) != 0)
      errorReady = ((eventMask & 
                     (select.EPOLLERR | select.EPOLLHUP)) != 0)
      eventCallback(fd = fd,
                    readReady = readReady,
                    writeReady = writeReady,
                    errorReady = errorReady)

class _KQueuePoller(_AbstractPoller):

  @staticmethod
  def _isAvailable():
    return hasattr(select, 'kqueue')

  def __init__(self):
    super().__init__()
    self.__kqueue = select.kqueue()
    self.__numFDs = 0

  def __str__(self):
    return ('KQueuePoller [ fileno = {} ]'.format(self.__kqueue.fileno()))

  @_signalSafe
  def __controlKqueue(self, changeList = None, maxEvents = 0, timeout = 0):
    return self.__kqueue.control(changeList, maxEvents, timeout)

  def _registerForEvents(self, fileno, readEvents, writeEvents):
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
    self.__controlKqueue(changeList = [readKE, writeKE])
    self.__numFDs += 1

  def _modifyRegistrationForEvents(self, fileno, readEvents, writeEvents):
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
    self.__controlKqueue(changeList = [readKE, writeKE])

  def _unregisterForEvents(self, fileno):
    readKE = select.kevent(ident = fileno,
                           filter = select.KQ_FILTER_READ,
                           flags = select.KQ_EV_DELETE)
    writeKE = select.kevent(ident = fileno,
                            filter = select.KQ_FILTER_WRITE,
                            flags = select.KQ_EV_DELETE)
    self.__controlKqueue(changeList = [readKE, writeKE])
    self.__numFDs -= 1

  def _poll(self, blockSeconds, eventCallback):
    eventList = self.__controlKqueue(
                  maxEvents = (self.__numFDs * 2),
                  timeout = blockSeconds)
    for ke in eventList:
      fd = ke.ident
      readReady = (ke.filter == select.KQ_FILTER_READ)
      writeReady = (ke.filter == select.KQ_FILTER_WRITE)
      errorReady = ((ke.flags & select.KQ_EV_EOF) != 0)
      eventCallback(fd = fd,
                    readReady = readReady,
                    writeReady = writeReady,
                    errorReady = errorReady)

class _PollPoller(_AbstractPoller):

  @staticmethod
  def _isAvailable():
    return hasattr(select, 'poll')

  def __init__(self):
    super().__init__()
    self.__poller = select.poll()

  def __str__(self):
    return 'PollPoller'

  def _registerForEvents(self, fileno, readEvents, writeEvents):
    eventMask = 0
    if (readEvents):
      eventMask |= select.POLLIN
    if (writeEvents):
      eventMask |= select.POLLOUT
    self.__poller.register(fileno, eventMask)

  def _modifyRegistrationForEvents(self, fileno, readEvents, writeEvents):
    eventMask = 0
    if (readEvents):
      eventMask |= select.POLLIN
    if (writeEvents):
      eventMask |= select.POLLOUT
    self.__poller.modify(fileno, eventMask)

  def _unregisterForEvents(self, fileno):
    self.__poller.unregister(fileno)

  @_signalSafe
  def __poll(self, blockSeconds):
    return self.__poller.poll(int(blockSeconds * 1000))

  def _poll(self, blockSeconds, eventCallback):
    readyList = self.__poll(blockSeconds)
    for (fd, eventMask) in readyList:
      readReady = ((eventMask & select.POLLIN) != 0)
      writeReady = ((eventMask & select.POLLOUT) != 0)
      errorReady = ((eventMask & 
                     (select.POLLERR | select.POLLHUP | select.POLLNVAL)) != 0)
      eventCallback(fd = fd,
                    readReady = readReady,
                    writeReady = writeReady,
                    errorReady = errorReady)

class _SelectPoller(_AbstractPoller):

  @staticmethod
  def _isAvailable():
    return hasattr(select, 'select')

  def __init__(self):
    super().__init__()
    self.__readFDSet = set()
    self.__writeFDSet = set()

  def __str__(self):
    return 'SelectPoller'

  def _registerForEvents(self, fileno, readEvents, writeEvents):
    self._modifyRegistrationForEvents(fileno, readEvents, writeEvents)

  def _modifyRegistrationForEvents(self, fileno, readEvents, writeEvents):
    if readEvents:
      self.__readFDSet.add(fileno)
    else:
      self.__readFDSet.discard(fileno)
    if writeEvents:
      self.__writeFDSet.add(fileno)
    else:
      self.__writeFDSet.discard(fileno)

  def _unregisterForEvents(self, fileno):
    self.__readFDSet.discard(fileno)
    self.__writeFDSet.discard(fileno)

  @_signalSafe
  def __poll(self, allFDSet, blockSeconds):
    return select.select(
      self.__readFDSet, self.__writeFDSet, allFDSet,
      blockSeconds)

  def _poll(self, blockSeconds, eventCallback):
    allFDSet = self.__readFDSet | self.__writeFDSet
    (readList, writeList, exceptList) = self.__poll(
      allFDSet = allFDSet,
      blockSeconds = blockSeconds)
    for fd in allFDSet:
      readReady = fd in readList
      writeReady = fd in writeList
      errorReady = fd in exceptList
      if (readReady or writeReady or errorReady):
        eventCallback(fd = fd,
                      readReady = readReady,
                      writeReady = writeReady,
                      errorReady = errorReady)

def createAsyncIOService(allow_epoll = True,
                         allow_kqueue = True,
                         allow_poll = True,
                         allow_select = True):

  '''Create an AsyncIOService supported by the platform and parameters.'''

  pollerClassesToTry = []

  if allow_epoll:
    pollerClassesToTry.append(_EPollPoller)

  if allow_kqueue:
    pollerClassesToTry.append(_KQueuePoller)

  if allow_poll:
    pollerClassesToTry.append(_PollPoller)

  if allow_select:
    pollerClassesToTry.append(_SelectPoller)

  poller = None
  for pollerClass in pollerClassesToTry:
    if pollerClass._isAvailable():
      poller = pollerClass()
      break

  if poller is None:
    raise AsyncException('Unable to create poller')

  return AsyncIOService(poller = poller)
