import sys
import logging
import configParser as config

logger = logging.getLogger('utils')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def lineContainsExpression(line, expression):
    if expression in line:
        return True
    else:
        return False


def fabOutputContainsExpression(fabOutput, expression):
    for line in fabOutput.iteritems():
        if lineContainsExpression(line, expression):
            return True

    return False


def generateClusterAddress():
    checkTopologyIsLoaded()
    clusterAddress = "gcomm://"
    for i in range(len(config.database_nodes)):
        nodeName = config.database_nodes[i]
        clusterAddress += nodeName

        if i < len(config.database_nodes) - 1:
            clusterAddress += ","

    return clusterAddress


def generateZookeeperConnectionString():
    connectionString = ''
    serversString = ''

    for i in range(len(config.coordinators_nodes)):
        nodeName = config.coordinators_nodes[i]
        connectionString += '{}:{}'.format(nodeName, config.ZOOKEEPER_CLIENT_PORT)
        serversString += 'server.{}={}:{}:{}\n'.format(i + 1, nodeName, config.ZOOKEEPER_PORT1, config.ZOOKEEPER_PORT2)

    if connectionString.endswith(','):
        connectionString = connectionString[:-1]

    config.ZOOKEEPER_CONNECTION_STRING = connectionString
    config.ZOOKEEPER_SERVERS_STRING = serversString


def checkTopologyIsLoaded():
    if not config.IS_LOADED:
        logger.error('topology file is not loaded')
        sys.exit()
