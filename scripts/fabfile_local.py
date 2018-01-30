from fabric.api import env, local, lcd, roles, parallel, cd, put, get, execute, settings, abort, hide, task, sudo, run, warn_only
import time
import sys
import xml.etree.ElementTree as ET
from sets import Set
import logging
import shlex
import subprocess, signal
import os
from parser import parseConfigInput

#------------------------------------------------------------------------------
# Deployment Scripts
# Author: David Lopes
# Nova University of Lisbon
# Last update: May, 2015
#------------------------------------------------------------------------------

#NUMBER_USERS_LIST=[1,3]
#NUMBER_REPLICAS=[3,5]
NUMBER_REPLICAS=[1]
#JDCBs=['mysql_crdt']
#NUMBER_USERS_LIST=[3,6,15,30,45,60]
NUMBER_USERS_LIST=[2]
#NUMBER_USERS_LIST=[1,3]
#NUMBER_REPLICAS=[1,3,5]
JDCBs=['mysql_crdt']
#JDCBs=['mysql_jdbc', 'mysql_crdt']

logger = logging.getLogger('simple_example')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

env.shell = "/bin/bash -l -i -c" 
env.user = 'dnl'
#env.user = 'dp.lopes'


# GLOBALS
CONFIG_FILE=''
LOG_FILE_DIR=''
TPCC_TEST_TIME=45
NUMBER_USERS=0


MYSQL_SHUTDOWN_COMMAND='bin/mysqladmin -u sa --password=101010 --socket=/tmp/mysql.sock shutdown'
MYSQL_START_COMMAND='bin/mysqld_safe --defaults-file=my.cnf'
TOMCAT_START='bin/startup.sh'
TOMCAT_SHUTDOWN_COMMAND='bin/shutdown.sh'

BASE_DIR = '/local/' + env.user
DEPLOY_DIR = BASE_DIR + '/deploy'
MYSQL_DIR = BASE_DIR + '/mysql-5.6'

HOME_DIR = '/home/' + env.user
LOGS_DIR = HOME_DIR + '/logs'
BACKUPS_DIR = HOME_DIR + '/backups'
PROJECT_DIR = HOME_DIR + '/code'
JARS_DIR = PROJECT_DIR + '/dist/jars'
EXPERIMENTS_DIR = PROJECT_DIR + '/experiments'

distinct_nodes = []
database_nodes = []
replicators_nodes = []
coordinators_nodes = []
proxies_nodes = []

configsMap = dict()
database_map = dict()
replicators_map = dict()
coordinators_map = dict()
proxies_map = dict()

replicatorsIdToPortMap = dict()
coordinatorsIdToPortMap = dict()
proxiesIdToPortMap = dict()

weakDBExperiment = False
env_vars = dict()
env.roledefs = {
    'configuratorNode': ["localhost"],
    'databases': database_nodes, 
    'replicators': replicators_nodes, 
    'coordinators': coordinators_nodes
}

env.hosts = ['localhost']

def runOriginExperiment():
    pass

def runWeakDBExperiment():
    pass    

@task
def benchmarkTPCC(configsFilesBaseDir):
    customJDBC=''
    global weakDBExperiment
    weakDBExperiment = False
    global LOG_FILE_DIR
    LOG_FILE_DIR = time.strftime("%H_%M_%S") + '_test_'                

    for replicasNum in NUMBER_REPLICAS:
        global CONFIG_FILE
        CONFIG_FILE = configsFilesBaseDir + '/'
        CONFIG_FILE += 'tpcc_localhost_' + str(replicasNum) + 'node.xml'
        #CONFIG_FILE += 'tpcc_cluster_' + str(replicasNum) + 'node.xml'
        logger.info('starting tests with %d replicas', replicasNum)
        parseConfigFile()
        
        killProcesses()            
        
        prepareCode()        
        for jdbc in JDCBs:
            if jdbc == 'mysql_crdt':
                weakDBExperiment = True
                customJDBC='true'
            else:
                customJDBC='false'     
                weakDBExperiment = False   
                    
            LOG_FILE_DIR += str(replicasNum) + 'replicas'
            for usersNum in NUMBER_USERS_LIST:                
                usersPerReplica = usersNum
                #usersPerReplica = usersNum / replicasNum
                global NUMBER_USERS
                NUMBER_USERS = usersNum                
                #LOG_FILE_DIR += str(replicasNum) + 'replicas_'
                #LOG_FILE_DIR += str(usersNum) + 'users/'                
                logger.info('this experiment will be logged to ' + LOG_FILE_DIR) 
                
                # preparar database
                logger.info('preparing tpcc database')
                with hide('running','output'):
                    execute(prepareTPCCDatabase, hosts=database_nodes)  

                #start databases
                with hide('running','output'):
                    dbResults = execute(startDatabases, hosts=database_nodes)
                    for key, value in dbResults.iteritems():
                        if value == '0':
                            logger.error('database at %s failed to start', key)
                            sys.exit()
                logger.info('all databases instances are online') 

                if weakDBExperiment:
                    #start coordinators
                    with hide('running','output'):
                        coordResults = execute(startCoordinators, hosts=coordinators_nodes)
                        for key, value in coordResults.iteritems():
                            if value == '0':
                                logger.error('coordinator at %s failed to start', key)
                                sys.exit()
                    logger.info('all coordinators are online')                           

                    #start replicators
                    with hide('running','output'):
                        replicatorResults = execute(startReplicators, hosts=replicators_nodes)
                        for key, value in replicatorResults.iteritems():
                            if value == '0':
                                logger.error('replicator at %s failed to start', key)
                                sys.exit()
                    logger.info('all replicators are online') 

                #start clients
                with hide('running','output'):
                    execute(startTPCCclients, usersPerReplica, customJDBC, hosts=proxies_nodes)
                
                if weakDBExperiment:
                    logger.info('running a experiment with our middleware') 
                else:
                    logger.info('running original experiment') 
                
                time.sleep(TPCC_TEST_TIME+30)   
                isRunning = True
                while isRunning:
                    logger.info('checking experiment status...')   
                    with hide('output','running'):
                        stillRunning = execute(checkClientsIsRunning, hosts=proxies_nodes)
                    for key, value in stillRunning.iteritems():
                        if value == '1':
                            isRunning = True
                            logger.info('experiment is still running!')                
                            break
                        else:
                            isRunning = False
                    if isRunning:
                        time.sleep(10)
                    else:
                        break                        
                logger.info('the experiment has finished!')
                killProcesses() 
                with cd(LOGS_DIR), hide('warnings'), settings(warn_only=True):       
                    run('mkdir -p ' + LOG_FILE_DIR)        

                execute(pushLogs, hosts=distinct_nodes)
                logger.info('this experiment has ended!')
                logger.info('merging log files')
                processLogFiles()
                logger.info('logs can be found at %s', LOG_FILE_DIR)
                logger.info('moving to the next iteration!')

            logger.info('generating plot for ' + str(replicasNum) + ' replicas experiment')
            generateLatencyThroughput()            

def generateLatencyThroughput():
    prefix = LOGS_DIR + '/' + LOG_FILE_DIR 
    with lcd(prefix):        
        for n in NUMBER_USERS_LIST:
            fileName = prefix + '/' + str(n) + '_clients.result'
            local('cat ' + fileName + ' >> plot_data')
            local('echo \'\' ' ' >> plot_data')
    
        plotFilePath = EXPERIMENTS_DIR + '/latency-throughput.gp' 
        local('gnuplot -e \"data=\'plot_data\'; outputfile=\'plot.eps\'\" ' + plotFilePath)

def prepareTPCW():
    if not is_mysql_running():
        mysql_start()

    time.sleep(1)
    if not is_mysql_running():
        print('mysql is not running. Exiting')
        sys.exit()

    export_file = HOME_DIR + '/backups/tpcw_export.sql'
    exportDatabase('tpcw', export_file)

@parallel
def startDatabases():
    command = 'nohup ' + MYSQL_START_COMMAND + ' >& /dev/null < /dev/null &'  
    logger.info('starting database: %s',command)
    with cd(MYSQL_DIR), hide('running','output'):    
        run(command)    
    time.sleep(15)
    if not isPortOpen('3306'):
        return '0'
    return '1'

@parallel
def startCoordinators():
    currentId = coordinators_map.get(env.host_string)    
    port = coordinatorsIdToPortMap.get(currentId)
    logFile = 'coordinator_' + str(currentId) + ".log"
    command = 'java -jar coordinator.jar ' + CONFIG_FILE + ' ' + currentId + ' > ' + logFile + ' &'
    logger.info('starting coordinator at %s', env.host_string)
    logger.info('%s',command)
    with cd(DEPLOY_DIR), hide('running','output'):
        run(command)
    time.sleep(15)
    if not isPortOpen(port):
        return '0'
    return '1'

@parallel
def startReplicators():
    currentId = replicators_map.get(env.host_string)    
    port = replicatorsIdToPortMap.get(currentId)
    logFile = 'replicator_' + str(currentId) + '_' + str(NUMBER_USERS) + 'users.log'
    command = 'java -jar replicator.jar ' + CONFIG_FILE + ' ' + currentId + ' > ' + logFile + ' &'
    logger.info('starting replicator at %s', env.host_string)
    logger.info('%s',command)
    with cd(DEPLOY_DIR), hide('running','output'):
        run(command)
    time.sleep(15)
    if not isPortOpen(port):
        return '0'
    return '1'

@parallel
def startTPCCclients(clientsNum, useCustomJDBC):
    currentId = proxies_map.get(env.host_string)    
    logFile = 'client_' + str(currentId) + '_' + str(NUMBER_USERS) + 'users.log'
    command = 'java -jar tpcc-client.jar ' + CONFIG_FILE + ' ' + currentId + ' ' + str(clientsNum) + ' ' + useCustomJDBC + ' ' + str(TPCC_TEST_TIME) + ' > ' + logFile + ' &'
    logger.info('starting client at %s', env.host_string)
    logger.info('%s',command)
    with cd(DEPLOY_DIR):
        run(command)  

def pushLogs():
    logger.info('%s is pushing log files to proper directory', env.host_string)
    
    with cd(DEPLOY_DIR), hide('warnings', 'output', 'running'), settings(warn_only=True):
        run('cp *.temp ' + LOGS_DIR + '/' + LOG_FILE_DIR)
        run('cp *.log ' + LOGS_DIR + '/' + LOG_FILE_DIR)

def killProcesses():
    logger.info('cleaning running processes')    
    with hide('running','output','warnings'):
        execute(stopJava, hosts=distinct_nodes)
        time.sleep(1)
        execute(stopMySQL, hosts=database_nodes)
        time.sleep(1)
        execute(stopJava, hosts=distinct_nodes)
        time.sleep(1)
        execute(stopMySQL, hosts=database_nodes)
        time.sleep(1)
    
def prepareCode():
    logger.info('compiling source code')
    with lcd(PROJECT_DIR), hide('output','running'):
        local('ant purge tpcc-dist')
    logger.info('uploading distribution to nodes: %s', distinct_nodes)
    logger.info('deploying jars, resources and config files')
    with hide('output','running'):
        execute(distributeCode, hosts=distinct_nodes)

def distributeCode():
    run('mkdir -p ' + DEPLOY_DIR)
    with cd(BASE_DIR), hide('output','running'), settings(warn_only=True):
        run('rm -rf ' + DEPLOY_DIR + '/*')                
        put(JARS_DIR + '/*.jar', DEPLOY_DIR)
        put(PROJECT_DIR + '/resources/configs', DEPLOY_DIR)
        put(PROJECT_DIR + '/experiments', DEPLOY_DIR)
        put(PROJECT_DIR + '/resources/*.sql', DEPLOY_DIR)
        put(PROJECT_DIR + '/resources/*.properties', DEPLOY_DIR)

def stopJava():
    command = 'ps ax | grep java'
    with settings(warn_only=True):
        output = run(command)
    for line in output.splitlines():                    
        if 'java' in line:        
            pid = int(line.split(None, 1)[0])            
            with settings(warn_only=True):
                run('kill -9 ' + str(pid))                
    
def stopMySQL():
    with settings(warn_only=True),hide('output'), cd(MYSQL_DIR):
        run(MYSQL_SHUTDOWN_COMMAND)

def is_mysql_running():
    with settings(warn_only=True),hide('output'):
        output = run('netstat -tan | grep 3306')
        return output.find('LISTEN') != -1

def isPortOpen(port):
    with settings(warn_only=True),hide('output'):
        output = run('netstat -tan | grep ' + port)
        return output.find('LISTEN') != -1

def parseConfigFile():
    logger.info('parsing config file: %s', CONFIG_FILE)
    e = ET.parse(CONFIG_FILE).getroot()
    distinctNodesSet = Set()
    global coordinators_map, replicatorsIdToPortMap, proxiesIdToPortMap, coordinatorsToPortMap
    global database_nodes
    global replicators_nodes
    global proxies_nodes
    global distinct_nodes
    global coordinators_nodes

    for database in e.iter('database'):
        dbId = database.get('id')
        dbHost = database.get('dbHost')
        database_nodes.append(dbHost)
        distinctNodesSet.add(dbHost)

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
        proxies_nodes.append(host)
        distinctNodesSet.add(host)
        proxiesIdToPortMap[proxyId] = port 
        proxies_map[host] = proxyId        

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
    logger.info('Clients: %s', proxies_nodes)
    logger.info('Replicators: %s', replicators_nodes)
    logger.info('Coordinators: %s', coordinators_nodes)
    logger.info('Distinct nodes: %s', distinct_nodes)

@parallel
def prepareTPCCDatabase():
    # assume mysql is not running
    logger.info('unpacking database at: %s', env.host_string)
    with cd(BASE_DIR), hide('output','running'):
        run('rm -rf mysql*')
        run('cp ' + BACKUPS_DIR + '/mysql-5.6_ready.tar.gz ' + BASE_DIR)
        run('tar zxvf mysql-5.6_ready.tar.gz')
    time.sleep(3)

@parallel
def checkClientsIsRunning():
    currentId = proxies_map.get(env.host_string)
    logFile = 'client_' + str(currentId) + '_' + str(NUMBER_USERS) + 'users.log'
    #logFile = 'client_' + str(currentId) + ".log"
    with cd(DEPLOY_DIR):
        output = run('tail ' + logFile)
        if 'CLIENT TERMINATED' not in output:
            return '0'
        else:
            return '1'


def processLogFiles():
    numberClients = len(proxies_map)
    totalOps = 0
    totalLatency = 0    
    prefix = LOGS_DIR + '/' + LOG_FILE_DIR 

    for x in xrange(1, numberClients+1):
        fileName = 'client_' + str(x) + '.result.temp'
        filePath = prefix + '/' + fileName
        lines = [line.strip() for line in open(filePath)]        
        splitted = lines[0].split(',')        
        parcialOps = int(splitted[0])
        totalOps += parcialOps
        parcialLatency = int(splitted[1])
        totalLatency += parcialLatency

    avgLatency = totalLatency / numberClients
    
    fileName = prefix + '/' + str(NUMBER_USERS) + '_clients.result'
               
    #OPS LATENCY CLIENTS
    with lcd(prefix):
        stringToWrite = str(totalOps) + ',' + str(avgLatency)
        f = open(fileName,'w')
        f.write(stringToWrite)
        f.close() # you can omit in most cases as the destructor will call if

        local('mkdir -p temp')
        local('mv *.temp temp/')








