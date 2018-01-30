#from fabric.api import env, local, roles, execute, settings
from fabric.api import env, local, lcd, roles, parallel, cd, put, get, execute, settings, abort, hide, task, sudo, run, warn_only
import datetime
import time
import sys
import logging
import plots
import configParser as config
import fabfile as fab
import utils as utils

logger = logging.getLogger('expLogger')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
TO_DOWNLOAD_COMMANDS = []

################################################################################################
# LATENCY-THROUGHPUT VARIABLES
################################################################################################
#NUMBER_REPLICAS=[1]
#JDCBs=['mysql_crdt', "default_jdbc"]
JDCBs=['cluster']
#JDCBs=['crdt','galera']
#JDCBs=['cluster']

#for 1 replica
#NUMBER_OF_USERS_LIST=[1,3,4,5,8,12,16]
#for 2~5 replica
NUMBER_OF_USERS_LIST=[1,3,4,5,8,12,16,20,25,30,35]
#NUMBER_OF_USERS_LIST=[1,3,4,5,8,12,16,20,25,30,35]
NUMBER_OF_USERS_LIST=[1,8]
#for 2 replicas
#NUMBER_OF_USERS_LIST=[1,3,4,5,8,12,16,20,25,30]
#for 3~5 replica
#NUMBER_OF_USERS_LIST=[1,3,4,5,8,12,16,20,25,30,35,45]
#NUMBER_OF_USERS_LIST=[45]
NUMBER_REPLICAS=[1]
NUMBER_USERS_LIST_1REPLICA=[1,2,3,4,6,8,10,14,18,24,30,32,40,50]
NUMBER_USERS_LIST_2REPLICA=[2,4,6,8,10,16,18,20,24,28,32,38,44,54,66,78,90,100,150]
NUMBER_USERS_LIST_3REPLICA=[3,6,9,12,15,18,21,24,27,30,36,45,54,66,81,102,120,144,162,180]
NUMBER_USERS_LIST_4REPLICA=[4,8,12,16,20,24,28,32,40,52,64,76,88,100,116,124,140,156,180,200,220,240]
NUMBER_USERS_LIST_5REPLICA=[5,10,15,20,25,30,35,45,55,70,90,110,140,170,210,250,300]

#userListToReplicasNumber = dict()
#userListToReplicasNumber[1] = NUMBER_USERS_LIST_1REPLICA
#userListToReplicasNumber[2] = NUMBER_USERS_LIST_2REPLICA
#userListToReplicasNumber[3] = NUMBER_USERS_LIST_3REPLICA
#userListToReplicasNumber[4] = NUMBER_USERS_LIST_4REPLICA
#userListToReplicasNumber[5] = NUMBER_USERS_LIST_5REPLICA

################################################################################################
# MAIN METHODS
################################################################################################

@task
def runExperiment(configsFilesBaseDir):
	config.ACTIVE_EXPERIMENT = config.prefix_latency_throughput_experiment
	# first cycle, iteration over the number of replicas
	for numberOfReplicas in NUMBER_REPLICAS:
		logger.info("###########################################################################################")
		logger.info("########################## STARTING NEW EXPERIMENT WITH %d REPLICAS ########################", numberOfReplicas)
		logger.info("###########################################################################################")
		print "\n"
		now = datetime.datetime.now()
		ROOT_OUTPUT_DIR = config.LOGS_DIR + "/" + now.strftime("%d-%m_%Hh%Mm%Ss_") + config.prefix_latency_throughput_experiment
		CONFIG_FILE_SUFFIX = str(config.ENVIRONMENT) + '_tpcc_' + str(numberOfReplicas) + 'node.xml'
		CONFIG_FILE = configsFilesBaseDir + '/' + str(config.ENVIRONMENT) + '_tpcc_' + str(numberOfReplicas) + 'node.xml'
		config.TOPOLOGY_FILE = config.TOPOLOGIES_DIR + '/' + CONFIG_FILE_SUFFIX
		REPLICA_OUTPUT_DIR = ROOT_OUTPUT_DIR

		config.parseTopologyFile(CONFIG_FILE)
		prepareCode()
		logger.info("starting tests with %d replicas", numberOfReplicas)

		# second cycle, use different jdbc to run experiment
		for jdbc in JDCBs:
			config.JDBC=jdbc
			fab.killRunningProcesses()
			if config.JDBC == 'cluster':
				success = False
				for attempt in range(10):
					success = populateClusterDatabase()
					if success:
						logger.info("mysql cluster database successfully populated")
						fab.killRunningProcesses()
						break
					else:
						logger.error("failed to populate mysql cluster database. Retrying...")
						fab.killRunningProcesses()
				if not success:
					logger.error("failed to populate mysql cluster database after 10 retries. Exiting.")
					sys.exit()

			# third cycle, use different number of users per run
			for numberOfUsers in NUMBER_OF_USERS_LIST:
				config.TOTAL_USERS = numberOfUsers*5
				OUTPUT_DIR = REPLICA_OUTPUT_DIR
				with hide('output','running','warnings'),settings(warn_only=True):
					local("mkdir -p " + OUTPUT_DIR + "/logs")
					get(config.ENVIRONMENT_FILE, OUTPUT_DIR)
					get(config.TOPOLOGY_FILE, OUTPUT_DIR)
					get(config.TPCC_WORKLOAD_FILE, OUTPUT_DIR)

				TOTAL_USERS = config.TOTAL_USERS
				NUMBER_OF_EMULATORS = len(config.emulators_nodes)
				USERS_PER_EMULATOR = numberOfUsers
				runLatencyThroughputExperiment(OUTPUT_DIR, CONFIG_FILE, NUMBER_OF_EMULATORS, USERS_PER_EMULATOR, TOTAL_USERS)
				logger.info('moving to the next iteration!')

	if not config.IS_LOCALHOST:
		scpCommand = "scp -r -P 12034 dp.lopes@di110.di.fct.unl.pt:"
		scpCommand += ROOT_OUTPUT_DIR
		scpCommand += " /Users/dnlopes/devel/thesis/code/weakdb/experiments/logs"
		TO_DOWNLOAD_COMMANDS.append(scpCommand)
		print "\n"
		logger.info("###########################################################################################")
		logger.info("all experiments have finished!")
		logger.info("use the following command to copy the logs directories:")
		logger.info(scpCommand)
		logger.info("###########################################################################################")
	print "\n"

@task
def killAll(driver,configFile):
	config.parseTopologyFile(configFile)
	config.JDBC=driver
	fab.killRunningProcesses()

################################################################################################
# LATENCY-THROUGHPUT METHODS
################################################################################################
def runLatencyThroughputExperiment(outputDir, configFile, numberEmulators, usersPerEmulator, totalUsers):

	print "\n"
	logger.info("########################################## Starting New Latency-Throughput Experiment ##########################################")
	logger.info('>> TOPOLOGY FILE: %s', config.TOPOLOGY_FILE)
	logger.info('>> DATABASES: %s', config.database_nodes)
	logger.info('>> REPLICATORS: %s', config.replicators_nodes)
	logger.info('>> EMULATORS: %s', config.emulators_nodes)
	logger.info('>> NUMBER OF EMULATORS: %s', numberEmulators)
	logger.info('>> CLIENTS PER EMULATOR: %s', usersPerEmulator)
	logger.info('>> TOTAL USERS: %s', totalUsers)
	logger.info('>> JDBC: %s', config.JDBC)
	logger.info('>> OUTPUT DIR: %s', outputDir)
	logger.info('>> WORKLOAD FILE: %s', config.TPCC_WORKLOAD_FILE)
	logger.info('>> ENVIRONMENT FILE: %s', config.ENVIRONMENT_FILE)
	logger.info("################################################################################################################################")
	print "\n"

	config.FILES_PREFIX = '{}_replicas_{}_users_{}_jdbc_'.format(len(config.replicators_nodes),totalUsers,config.JDBC)

	success = False
	for attempt in range(10):
		if config.JDBC == 'crdt':
			success = runLatencyThroughputExperimentCRDT(outputDir, configFile, numberEmulators, usersPerEmulator, totalUsers)
		elif config.JDBC == 'galera':
			success = runLatencyThroughputExperimentBaseline(outputDir, configFile, numberEmulators, usersPerEmulator, totalUsers)
		elif config.JDBC == 'cluster':
			success = runLatencyThroughputExperimentCluster(outputDir, configFile, numberEmulators, usersPerEmulator, totalUsers)

		if success:
			break
		else:
			logger.error("experiment failed. Retrying...")
			fab.killRunningProcesses()
			execute(fab.cleanOutputFiles, hosts=config.distinct_nodes)

	if not success:
		logger.error("failed to execute experiment after 10 retries. Exiting...")
		sys.exit()

def runLatencyThroughputExperimentCRDT(outputDir, configFile, numberEmulators, usersPerEmulator, totalUsers):
	logger.info("starting database layer")

	success = startDatabaseLayer()

	if success:
		logger.info("all databases instances are online")
	else:
		logger.error("database layer failed to start. Exiting")
		return False

	preloadDatabases('tpcc_crdt')
	logger.info("all databases are loaded into memory")

	success = startCoordinatorsLayer()
	if success:
		logger.info('all coordinators are online')
	else:
		logger.error("coordination layer failed to start. Exiting")
		return False

	success = startReplicationLayer(configFile)
	if success:
		logger.info('all replicators are online')
	else:
		logger.error("replication layer failed to start. Exiting")
		return False

	startClientEmulators(configFile, numberEmulators, usersPerEmulator, "true")

	time.sleep(config.TPCC_TEST_TIME+config.TPCC_RAMP_UP_TIME+20)
	isRunning = True
	attempts = 0

	while isRunning:
		if attempts >= 10:
			logger.error("checked 10 times if clients were running. Something is probably wrong")
			return False
		logger.info('checking experiment status...')
		with hide('running', 'output'):
			output = execute(fab.areClientsRunning, numberEmulators, hosts=config.emulators_nodes)
			if utils.fabOutputContainsExpression(output, "True"):
				isRunning = True
				logger.info('experiment is still running!')
			else:
				isRunning = False
		if isRunning:
			attempts += 1
			time.sleep(15)
		else:
			break

	logger.info('the experiment has finished!')
	fab.killRunningProcesses()
	downloadLogs(outputDir)

	return True

def runLatencyThroughputExperimentBaseline(outputDir, configFile, numberEmulators, usersPerEmulator, totalUsers):

	logger.info("starting database layer")

	success = startDatabaseLayer()

	if success:
		logger.info("all databases instances are online")
	else:
		logger.error("database layer failed to start")
		return False

	preloadDatabases('tpcc')
	logger.info("all databases are loaded into memory")
	startClientEmulators(configFile, numberEmulators, usersPerEmulator, "false")

	time.sleep(config.TPCC_TEST_TIME+config.TPCC_RAMP_UP_TIME+20)
	isRunning = True
	attempts = 0

	while isRunning:
		if attempts >= 10:
			logger.error("checked 10 times if clients were running. Something is probably wrong")
			return False
		logger.info('checking experiment status...')
		with hide('running', 'output'):
			output = execute(fab.areClientsRunning, numberEmulators, hosts=config.emulators_nodes)
			if utils.fabOutputContainsExpression(output, "True"):
				isRunning = True
				logger.info('experiment is still running!')
			else:
				isRunning = False
		if isRunning:
			attempts += 1
			time.sleep(15)
		else:
			break

	logger.info('the experiment has finished!')
	fab.killRunningProcesses()
	downloadLogs(outputDir)
	#plots.mergeTemporaryCSVfiles(outputDir, totalUsers, numberEmulators)
	logger.info('logs can be found at %s', outputDir)

	return True

def runLatencyThroughputExperimentCluster(outputDir, configFile, numberEmulators, usersPerEmulator, totalUsers):

	logger.info("starting database layer")

	success = startDatabaseLayer()
	if success:
		logger.info("all databases instances are online")
	else:
		logger.error("database layer failed to start")
		return False

	preloadDatabases('tpcc')
	logger.info("all databases are loaded into memory")
	startClientEmulators(configFile, numberEmulators, usersPerEmulator, "false")

	time.sleep(config.TPCC_TEST_TIME+config.TPCC_RAMP_UP_TIME+20)
	isRunning = True
	attempts = 0
	while isRunning:
		if attempts >= 10:
			logger.error("checked 10 times if clients were running. Something is probably wrong")
			return False
		logger.info('checking experiment status...')
		with hide('running', 'output'):
			output = execute(fab.areClientsRunning, numberEmulators, hosts=config.emulators_nodes)
			if utils.fabOutputContainsExpression(output, "True"):
				isRunning = True
				logger.info('experiment is still running!')
			else:
				isRunning = False
		if isRunning:
			attempts += 1
			time.sleep(10)
		else:
			break

	logger.info('the experiment has finished!')
	fab.killRunningProcesses()
	downloadLogs(outputDir)
	#plots.mergeTemporaryCSVfiles(outputDir, totalUsers, numberEmulators)
	logger.info('logs can be found at %s', outputDir)

	return True

################################################################################################
#   START LAYERS METHODS
################################################################################################
def startDatabaseLayer():
	if config.JDBC == 'crdt':
		with hide('running','output'):
			execute(fab.prepareDatabase, hosts=config.database_nodes)
			output = execute(fab.startDatabases, hosts=config.database_nodes)
			for key, value in output.iteritems():
				if value == '0':
					logger.warn('database at %s failed to start', key)
					return False
			return True
	elif config.JDBC == 'galera':
		with hide('running','output'):
			execute(fab.prepareDatabase, hosts=config.database_nodes)
			masterDatabaseReplica = config.database_nodes[0]
			masterList = [masterDatabaseReplica]
			slavesReplicas = config.database_nodes[:]
			slavesReplicas.remove(masterDatabaseReplica)
			logger.info("%s will bootstrap Galera-Cluster", masterDatabaseReplica)
			logger.info("%s will join after the cluster is online", slavesReplicas)
			#start master replica (that will bootstrap the cluster)
			output = execute(fab.startDatabasesGalera, True, hosts=masterList)
		for key, value in output.iteritems():
			if utils.fabOutputContainsExpression(output, "0"):
				logger.error('database at %s failed to start', key)
				return False

		if len(config.database_nodes) > 1:
			#start remainning nodes
			with hide('running','output'):
				output = execute(fab.startDatabasesGalera, False, hosts=slavesReplicas)
			for key, value in output.iteritems():
				if utils.fabOutputContainsExpression(output, "0"):
					logger.error('database at %s failed to start', key)
					return False

		return True

	elif config.JDBC == 'cluster':
		with hide('running','output'):
			execute(fab.prepareDatabase, hosts=config.database_nodes)
			output = fab.startClusterDatabases(False)
			if output == '0':
				logger.warn('mysql cluster failed to start')
				return False
			return True
	else:
		logger.error("unexpected driver: %s", config.JDBC)
		sys.exit()

def startCoordinatorsLayer():

	with hide('running','output'):
		execute(fab.prepareCoordinatorLayer, hosts=config.coordinators_nodes)
		output = execute(fab.startCoordinators, hosts=config.coordinators_nodes)
		for key, value in output.iteritems():
			if value == '0':
				logger.error('coordinator at %s failed to start', key)
				return False
	return True

def startReplicationLayer(configFile):
	with hide('running','output'):
		output = execute(fab.startReplicators, configFile, hosts=config.replicators_nodes)
		for key, value in output.iteritems():
			if value == '0':
				logger.error('replicator at %s failed to start', key)
				return False
	return True

def startClientEmulators(configFile, emulatorsNumber, clientsPerEmulator, customJDBC):
	with hide('running','output'):
		execute(fab.startTPCCclients, configFile, emulatorsNumber, clientsPerEmulator, customJDBC, hosts=config.emulators_nodes)

################################################################################################
#   HELPER AND "PRIVATE" METHODS
################################################################################################
def prepareCode():
	logger.info('compiling source code')
	command = 'ant purge deploy'
	fab.printExecution(command, env.host_string)
	fab.executeTerminalCommandAtDir(command, config.PROJECT_DIR)
	logger.info('uploading distribution to nodes: %s', config.distinct_nodes)
	logger.info('deploying jars, resources and config files')
	with hide('output','running'):
		execute(fab.distributeCode, hosts=config.distinct_nodes)

def downloadLogs(outputDir):
	#logger.info('downloading log files')
	with hide('running', 'output'):
		execute(fab.downloadLogsTo, outputDir, hosts=config.distinct_nodes)

def checkGaleraClusterStatus(masterReplicaHost):
	numberOfDatabases = len(config.database_nodes)
	command = 'bin/mysql --defaults-file=my.cnf -u sa -p101010 -e "SHOW STATUS LIKE \'wsrep_cluster_size\';" | grep wsrep'
	output = fab.executeRemoteTerminalCommandAtDir(masterReplicaHost, command, config.GALERA_MYSQL_DIR)
	logger.debug('cluster output: %s', output)

	if str(numberOfDatabases) not in output:
		logger.error("cluster was not properly initialized: %s", output)
		return True

	command = 'bin/mysql --defaults-file=my.cnf -u sa -p101010 -e "SHOW STATUS LIKE \'wsrep_ready\';" | grep wsrep_ready'
	output = fab.executeRemoteTerminalCommandAtDir(masterReplicaHost, command, config.GALERA_MYSQL_DIR)
	if 'ON' not in output:
		logger.error("cluster was not properly initialized: %s", output)
		return True

	return True

def preloadDatabases(dbName):
	with hide('running','output'):
		execute(fab.preloadDatabase, dbName, hosts=config.database_nodes)

def populateClusterDatabase():
	logger.info("preparing mysql cluster database")
	with hide('running','output'):
		execute(fab.prepareDatabase, hosts=config.database_nodes)
		output = fab.startClusterDatabases(True)
		if output == '0':
			logger.warn('mysql cluster failed to start')
			return False

	logger.info("populating now")
	success = execute(fab.populateTpccDatabase, hosts=config.MYSQL_CLUSTER_MGMT_NODES_LIST)
	for key, value in success.iteritems():
		if value == '0':
			return False

	logger.info("populating process done")
	with hide('running','output'):
		fab.killRunningProcesses()
		execute(fab.compressDataNodeFolder, hosts=config.database_nodes)
	return True