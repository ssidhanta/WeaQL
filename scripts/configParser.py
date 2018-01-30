import xml.etree.ElementTree as ET
from sets import Set
import logging
import utils as utils

logger = logging.getLogger('globalVar_logger')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

IS_LOADED = False
IS_LOCALHOST = False

################################################################################################
#   CURRENT CONFIGURATION (the only variables needed to modify between benchmarks)
################################################################################################

#user='ubuntu'
user='dp.lopes'
#user='dnl'

ENVIRONMENT='fct'
#ENVIRONMENT='amazon'
#ENVIRONMENT='localhost'
WAREHOUSES_NUMBER=3

if ENVIRONMENT == 'localhost':
    IS_LOCALHOST = True

################################################################################################
#	COMMANDS AND BASE PATHS
################################################################################################
WEAK_DB_LOG4J_FILE = "-Dlog4j.configuration=file:\"./log4j_weakdb.properties\""
ZOOKEEPER_LOG4J_FILE = "-Dlog4j.configuration=file:\"./log4j_zookeeper.properties\""

MYSQL_SHUTDOWN_COMMAND='bin/mysqladmin -u sa --password=101010 --socket=/tmp/mysql.sock shutdown'
MYSQL_START_COMMAND='bin/mysqld_safe --defaults-file=my.cnf --open_files_limit=8192 --max-connections=1500 --innodb_buffer_pool_size=8G'
BASE_DIR = '/local/' + user
DEPLOY_DIR = BASE_DIR + '/deploy'
MYSQL_CLUSTER_DATA_DIR = BASE_DIR + '/cluster-data'
ENVIRONMENT_DIR = DEPLOY_DIR + '/environment'
ANNOTATIONS_DIR = DEPLOY_DIR + '/annotations'
TOPOLOGIES_DIR = DEPLOY_DIR + '/topologies'
TPCC_WORKLOADS_DIR = DEPLOY_DIR + '/tpcc'
MYSQL_DIR = BASE_DIR + '/mysql-5.6'
MYSQL_CLUSTER_DIR = BASE_DIR + '/mysql-cluster-7.0'
GALERA_MYSQL_DIR = BASE_DIR + '/mysql-5.6-galera'
CLUSTER_MYSQL_DIR = BASE_DIR + '/mysql-cluster'
HOME_DIR = '/home/' + user
LOGS_DIR = HOME_DIR + '/logs'
BACKUPS_DIR = HOME_DIR + '/backups'
PROJECT_DIR = HOME_DIR + '/code'
JARS_DIR = PROJECT_DIR + '/dist/jars'
EXPERIMENTS_DIR = PROJECT_DIR + '/experiments'
ZOOKEEPER_DATA_DIR = BASE_DIR + '/zookeeper'

TPCC_WORKLOAD_FILE= TPCC_WORKLOADS_DIR + '/workload1'

#ENVIRONMENT_FILE= ENVIRONMENT_DIR + '/env_localhost_tpcc_default_nocoord.env'
ENVIRONMENT_FILE= ENVIRONMENT_DIR + '/env_localhost_tpcc_default_coord.env'

TOPOLOGY_FILE=''
TPCC_TEST_TIME=60
TPCC_RAMP_UP_TIME=10
FILES_PREFIX=''



################################################################################################
#	MYSQL CLUSTER COMMANDS
################################################################################################
MYSQL_CLUSTER_CONNECTION_STRING='node1'
MYSQL_CLUSTER_MGMT_NODES_LIST = ['node1']
CLUSTER_START_MGMG_COMMAND='bin/ndb_mgmd --initial --config-dir=' + MYSQL_CLUSTER_DIR + '/conf -f '
CLUSTER_START_DATA_NODE_COMMAND='bin/ndbd -c ' + MYSQL_CLUSTER_CONNECTION_STRING
CLUSTER_MYSQL_START_COMMAND='bin/mysqld_safe --defaults-file=my.cnf --open_files_limit=8192 --max-connections=1500 --innodb_buffer_pool_size=8G --ndbcluster --ndb-connectstring=' + MYSQL_CLUSTER_CONNECTION_STRING
################################################################################################
#   PREFIXS AND GLOBAL VARIABLES
################################################################################################
ZOOKEEPER_CLIENT_PORT='2181'
ZOOKEEPER_PORT1 ='2888'
ZOOKEEPER_PORT2 ='3888'
ZOOKEEPER_CFG_FILE='zoo.cfg'
ZOOKEEPER_CONNECTION_STRING = ''
ZOOKEEPER_SERVERS_STRING = ''

MYSQL_PORT='3306'
MYSQL_CLUSTER_MGMT_NODE_PORT='1186'
MYSQL_CLUSTER_DATA_NODE_PORT='3406'
TOTAL_USERS=0
JDBC=''

ACTIVE_EXPERIMENT=""
prefix_latency_throughput_experiment = "latency-throughput"
prefix_scalability_experiment = "scalability"
prefix_overhead_experiment = "overhead"

################################################################################################
#	DATA STRUCTURES
################################################################################################

distinct_nodes = []
database_nodes = []
replicators_nodes = []
coordinators_nodes = []
emulators_nodes = []

# maps between node_id and node_host
database_map = dict()
replicators_map = dict()
coordinators_map = dict()
emulators_map = dict()
emulators_instances_count = dict()
emulators_workloads = dict()
# maps between node_id and listening port
# usefull for checking if the layer was correctly initialized
replicatorsIdToPortMap = dict()
coordinatorsIdToPortMap = dict()

################################################################################################
#	METHODS
################################################################################################

# receives full path for config file
def parseTopologyFile(topologyFile):
    logger.info('parsing topology file: %s', topologyFile)
    e = ET.parse(topologyFile).getroot()
    distinctNodesSet = Set()

    global database_map, emulators_map, coordinators_map, replicators_map, replicatorsIdToPortMap, coordinatorsIdToPortMap
    global database_nodes, replicators_nodes, distinct_nodes, coordinators_nodes, emulators_nodes,emulators_instances_count,emulators_workloads

    distinct_nodes = []
    database_nodes = []
    replicators_nodes = []
    coordinators_nodes = []
    emulators_nodes = []
    database_map = dict()
    replicators_map = dict()
    coordinators_map = dict()
    emulators_map = dict()
    replicatorsIdToPortMap = dict()
    coordinatorsIdToPortMap = dict()
    emulators_instances_count = dict()
    emulators_workloads = dict()

    for database in e.iter('database'):
        dbId = database.get('id')
        dbHost = database.get('dbHost')
        database_nodes.append(dbHost)
        distinctNodesSet.add(dbHost)
        database_map[dbHost] = dbId

    for replicator in e.iter('replicator'):
        replicatorId = replicator.get('id')
        host = replicator.get('host')
        port = replicator.get('port')
        replicators_nodes.append(host)
        distinctNodesSet.add(host)
        replicatorsIdToPortMap[replicatorId] = port
        replicators_map[host] = replicatorId

    for proxy in e.iter('proxy'):
        proxyId = proxy.get('id')
        host = proxy.get('host')
        port = proxy.get('port')
        workload = proxy.get('workloadFile')
        emulators_nodes.append(host)
        distinctNodesSet.add(host)
        emulators_map[host] = proxyId
    #emulators_workloads[proxyId] = workload
    #if not host in emulators_instances_count:
    #		emulators_instances_count[host] = 1
    #else:
    #		emulators_instances_count[host] += 1

    for coordinator in e.iter('coordinator'):
        coordinatorId = coordinator.get('id')
        port = coordinator.get('port')
        host = coordinator.get('host')
        coordinators_nodes.append(host)
        distinctNodesSet.add(host)
        coordinators_map[host] = coordinatorId
        coordinatorsIdToPortMap[coordinatorId] = port

    distinct_nodes = list(distinctNodesSet)

    logger.info('Databases: %s', database_nodes)
    logger.info('Coordinators: %s', coordinators_nodes)
    logger.info('Replicators: %s', replicators_nodes)
    logger.info('Emulators: %s', emulators_nodes)
    logger.info('Distinct nodes: %s', distinct_nodes)

    utils.generateZookeeperConnectionString()
    global IS_LOADED
    IS_LOADED = True







