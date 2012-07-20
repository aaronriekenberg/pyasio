#!/usr/bin/env python3

import asio
import logging
import os
import sys

MAX_READ_BYTES = 256 * 1024

def createLogger():
  logger = logging.getLogger('proxy')
  logger.setLevel(logging.INFO)

  consoleHandler = logging.StreamHandler()
  consoleHandler.setLevel(logging.DEBUG)

  formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  consoleHandler.setFormatter(formatter)

  logger.addHandler(consoleHandler)

  return logger

logger = createLogger()

class Connection(object):

  def __init__(self, ioService, clientToProxySocket,
               remoteAddress, remotePort):
    super().__init__()
    self.__writingToClient = False
    self.__writingToRemote = False
    self.__clientToProxySocket = clientToProxySocket
    self.__clientToProxyString = '{0} -> {1}'.format(
      clientToProxySocket.getpeername(),
      clientToProxySocket.getsockname())
    self.__proxyToRemoteSocket = ioService.createAsyncSocket()
    self.__proxyToRemoteString = ''
    self.__proxyToRemoteSocket.asyncConnect(
      (remoteAddress, remotePort), 
      self.__connectCallback)

  def close(self):
    if not self.__writingToClient:
      if ((not self.__clientToProxySocket.closed()) and
          (len(self.__clientToProxyString) > 0)):
        logger.info('disconnect {0} (fd={1})'.format(
                    self.__clientToProxyString,
                    self.__clientToProxySocket.fileno()))
      self.__clientToProxySocket.close()
    if not self.__writingToRemote:
      if ((not self.__proxyToRemoteSocket.closed()) and
          (len(self.__proxyToRemoteString) > 0)):
        logger.info('disconnect {0} (fd={1})'.format(
                    self.__proxyToRemoteString,
                    self.__proxyToRemoteSocket.fileno()))
      self.__proxyToRemoteSocket.close()

  def __connectCallback(self, error):
    if (error != 0):
      logger.info('connect error \'{0}\''.format(os.strerror(error)))
      self.close()
    else:
      self.__proxyToRemoteString = '{0} -> {1}'.format(
        self.__proxyToRemoteSocket.getpeername(),
        self.__proxyToRemoteSocket.getsockname())
      logger.info('connect {0} (fd={1})'.format(
                  self.__proxyToRemoteString,
                  self.__proxyToRemoteSocket.fileno()))
      self.__clientToProxySocket.asyncRead(
        MAX_READ_BYTES,
        self.__readFromClientCallback)
      self.__proxyToRemoteSocket.asyncRead(
        MAX_READ_BYTES,
        self.__readFromRemoteCallback)

  def __readFromClientCallback(self, data, error):
    if self.__proxyToRemoteSocket.closed():
      self.close()
    elif (error != 0):
      self.close()
    elif not data:
      self.close()
    else:
      self.__writingToRemote = True
      self.__proxyToRemoteSocket.asyncWriteAll(data, self.__writeToRemoteCallback)

  def __readFromRemoteCallback(self, data, error):
    if self.__clientToProxySocket.closed():
      self.close()
    elif (error != 0):
      self.close()
    elif not data:
      self.close()
    else:
      self.__writingToClient = True
      self.__clientToProxySocket.asyncWriteAll(data, self.__writeToClientCallback)

  def __writeToRemoteCallback(self, error):
    self.__writingToRemote = False
    if self.__clientToProxySocket.closed():
      self.close()
    elif (error != 0):
      self.close()
    else:
      self.__clientToProxySocket.asyncRead(MAX_READ_BYTES, self.__readFromClientCallback)

  def __writeToClientCallback(self, error):
    self.__writingToClient = False
    if self.__proxyToRemoteSocket.closed():
      self.close()
    elif (error != 0):
      self.close()
    else:
      self.__proxyToRemoteSocket.asyncRead(MAX_READ_BYTES, self.__readFromRemoteCallback)

class Acceptor(object):

  def __init__(self, ioService,
               localAddress, localPort,
               remoteAddress, remotePort):
    super().__init__()
    self.__ioService = ioService
    self.__remoteAddress = remoteAddress
    self.__remotePort = remotePort
    self.__asyncSocket = ioService.createAsyncSocket();
    self.__asyncSocket.setReuseAddress()
    self.__asyncSocket.bind((localAddress, localPort))
    self.__asyncSocket.listen()
    self.__asyncSocket.asyncAccept(self.__acceptCallback)
    logger.info('listening on {0} (fd={1})'.format(
                self.__asyncSocket.getsockname(),
                self.__asyncSocket.fileno()))

  def __acceptCallback(self, asyncSocket, error):
    if ((error == 0) and (asyncSocket is not None)):
      logger.info('accept {0} -> {1} (fd={2})'.format(
                  asyncSocket.getpeername(),
                  asyncSocket.getsockname(),
                  asyncSocket.fileno()))
      Connection(
        self.__ioService, asyncSocket, self.__remoteAddress, self.__remotePort)
    self.__asyncSocket.asyncAccept(self.__acceptCallback)

def parseAddrPortString(addrPortString):
  addrPortList = addrPortString.split(':', 1)
  return (addrPortList[0], int(addrPortList[1]))

def printUsage():
  logger.error(
    'Usage: {0} <listen addr> [<listen addr> ...] <remote addr>'.format(
      sys.argv[0]))

def main():
  if (len(sys.argv) < 3):
    printUsage()
    sys.exit(1)

  localAddressPortList = map(parseAddrPortString, sys.argv[1:-1])
  (remoteAddress, remotePort) = parseAddrPortString(sys.argv[-1])

  ioService = asio.createAsyncIOService()
  logger.info('ioService = {0}'.format(ioService))
  for (localAddress, localPort) in localAddressPortList:
    Acceptor(ioService,
             localAddress = localAddress,
             localPort = localPort,
             remoteAddress = remoteAddress,
             remotePort = remotePort)
  logger.info('remote address {0}'.format((remoteAddress, remotePort)))
  ioService.run()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    pass
