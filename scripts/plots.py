import sys
import logging
import glob
import pandas as pd
import configParser as config
import os.path
import fabfile as fab
from optparse import OptionParser

logger = logging.getLogger('plotsLogger')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s] %(message)s')
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

################################################################################################
#   VARIABLES
################################################################################################

LATENCY_THROUGHPUT_1LINE = "latency-throughput_1line.gp"
LATENCY_THROUGHPUT_2LINE = "latency-throughput_2line.gp"
SCALABILITY_1LINE = "scalability_1line.gp"
SCALABILITY_2LINE = "scalability_2line.gp"
OVERHEAD = "overhead_chart.gp"

parser = OptionParser()
parser.add_option("-d", "--dir", dest="outputDir",
				  help="CSVs files directory")
parser.add_option("-g", "--graphic", help="plot to generate \n (1) plotLatencyThroughputGraphic (2) generateScalabilityPlot")



################################################################################################
# LATENCY-THROUGHPUT METHODS
################################################################################################


def generateDataFile(dataDir):
	workloadFile = glob.glob(outputDir + "/*workload*")
	if len(workloadFile) == 0:
		logger.error("workload file not found. Exiting")
		sys.exit()
	if len(workloadFile) > 1:
		logger.error("multiple workload files found! Exiting")
		sys.exit()

	d = {}
	with open(workloadFile) as f:
		for line in f:
			key, value = line.split('=')
			d[key] = value

	workloadName = d['name']
	csvFiles = glob.glob(outputDir + "/*.csv")

	print csvFiles
	sys.exit()


def plotLatencyThroughputGraphic(outputDir):
	workloadFile = glob.glob(outputDir + "/*workload*")
	if len(workloadFile) == 0:
		logger.error("workload file not found. Exiting")
		sys.exit()
	if len(workloadFile) > 1:
		logger.error("multiple workload files found! Exiting")
		sys.exit()

	d = {}
	with open(workloadFile) as f:
		for line in f:
			key, value = line.split('=')
			d[key] = value

	workloadName = d['name']
	csvFiles = glob.glob(outputDir + "/*.csv")

	print csvFiles
	sys.exit()

def generateLatencyThroughputPlot(outputDir):
	plotDataFiles = glob.glob(outputDir + "/*.csv")
	logger.info("generating latency-throughput plot with the following datapoints files: %s", plotDataFiles)

	if len(plotDataFiles) == 1:
		logger.info("generating plot with 1 line")
		plotCommand = 'gnuplot -e \"data1=\''
		plotCommand += plotDataFiles[0]
		plotCommand += '\'; outputfile=\'latency-throughput.eps\'\" '
		plotCommand += config.EXPERIMENTS_DIR + "/" + LATENCY_THROUGHPUT_1LINE
		fab.executeTerminalCommandAtDir(plotCommand, outputDir)
	elif len(plotDataFiles) == 2:
		logger.info("generating plot with 2 line")
		plotCommand = 'gnuplot -e \"data1=\''
		if 'crdt' in plotDataFiles[0]:
			plotCommand += plotDataFiles[0]
		elif 'crdt' in plotDataFiles[1]:
			plotCommand += plotDataFiles[1]
		plotCommand += '\'; data2=\''
		if 'galera' in plotDataFiles[0]:
			plotCommand += plotDataFiles[0]
		elif 'galera' in plotDataFiles[1]:
			plotCommand += plotDataFiles[1]

		plotCommand += '\' ; outputfile=\'latency-throughput.eps\'\" '
		plotCommand += config.EXPERIMENTS_DIR + "/" + LATENCY_THROUGHPUT_2LINE
		fab.executeTerminalCommandAtDir(plotCommand, outputDir)
	else:
		logger.error("unexpected number of csv files to plot latency-throughput graphic")
		sys.exit()


def generateLatencyThroughputDataFile(outputDir, usersList):
	frame = pd.DataFrame()
	list_ = []
	foundFile = False

	for numberUsers in usersList:
		dirName = outputDir + "/" + str(numberUsers) + "user"
		fileName = dirName + "/" + str(numberUsers)

		fileName += "latency-throughput_"
		fileName += config.JDBC
		fileName += ".csv"

		if not os.path.isfile(fileName):
			logger.warn("file %s not available", fileName)
			continue
		df = pd.read_csv(fileName, index_col=False)
		list_.append(df)
		foundFile = True

	if foundFile:
		frame = pd.concat(list_)
		fileName = outputDir + "/latency-throughput_datapoints_"
		fileName += config.JDBC
		fileName += ".csv"

		frame.to_csv(fileName, sep=",", index=False)


################################################################################################
# OVERHEAD METHODS
################################################################################################
def generateOverheadPlot(outputDir, usersList, jdbcDriversList):
	logger.info("generating overhead plot for drivers: %s", jdbcDriversList)
	generateOverheadDatafile(outputDir, usersList, jdbcDriversList)

	dataFile = outputDir + "/" + "overhead_data.csv"
	plotCommand = 'gnuplot -e \"data1=\''
	plotCommand += dataFile
	plotCommand += '\'; outputfile=\'overhead_data.eps\'\" '
	plotCommand += config.EXPERIMENTS_DIR + "/" + OVERHEAD
	fab.executeTerminalCommandAtDir(plotCommand, outputDir)


def generateOverheadDatafile(outputDir, usersList, jdbcDriversList):
	dataFileContent = "Users,Default,CRDT\n"
	for userNum in usersList:
		CURRENT_DIR = outputDir + "/" + str(userNum) + "user"
		entry = str(userNum)

		for jdbc in jdbcDriversList:
			csvFileName = CURRENT_DIR + "/" + str(userNum) + config.ACTIVE_EXPERIMENT + "_" + str(jdbc) + ".csv"
			df = pd.read_csv(csvFileName, index_col=None, header=0)
			avgLatency = df['avgLatency'].sum()
			entry += "," + str(avgLatency)

		entry += "\n"
		dataFileContent += entry

	fileName = "overhead_data.csv"
	f = open(fileName, 'w')
	f.write(dataFileContent)
	f.close()


################################################################################################
# SCALABILITY METHODS
################################################################################################
def generateScalabilityPlot(outputDir, numberReplicasList, jdbcDriversList):
	for driver in jdbcDriversList:
		logger.debug("generating scalability plot data for driver: %s", driver)
		generateScalabilityDataPointsForJDBC(outputDir, driver, numberReplicasList)

	logger.info("generating scalability plot for %s drivers: ", jdbcDriversList)
	plotDataFiles = glob.glob(outputDir + "/*.csv")

	if len(plotDataFiles) == 1:
		logger.info("generating plot with 1 line")
		plotCommand = 'gnuplot -e \"data1=\''
		plotCommand += plotDataFiles[0]
		plotCommand += '\'; outputfile=\'scalability.eps\'\" '
		plotCommand += config.EXPERIMENTS_DIR + "/" + SCALABILITY_1LINE
		fab.executeTerminalCommandAtDir(plotCommand, outputDir)
	elif len(plotDataFiles) == 2:
		logger.warn("missing implementation")
		sys.exit()
	else:
		logger.error("unexpected number of csv files to plot graphic")
		sys.exit()


def generateScalabilityDataPointsForJDBC(outputDir, jdbcDriver, numberReplicasList):
	csvFiles = glob.glob(outputDir + "/*.results.temp")

	frame = pd.DataFrame()
	list_ = []
	foundFile = False
	for replicaNum in numberReplicasList:
		aDir = outputDir + "/" + str(replicaNum) + "replica"
		fileSufix = jdbcDriver + ".csv"
		fullDir = aDir + "/*" + fileSufix
		csvFiles = glob.glob(fullDir)

		if len(csvFiles) != 1:
			logger.error("there should exist exactly one CSV file for each jdcb driver per replica folder at: %s",
						 fullDir)
			sys.exit()

		csvFileName = csvFiles[0]
		df = pd.read_csv(csvFileName, index_col=None, header=0)
		list_.append(df)
		foundFile = True

	if foundFile:
		frame = pd.concat(list_)
		fileName = outputDir + "/scalability_datapoints_"
		fileName += config.JDBC
		fileName += ".csv"
		frame.to_csv(fileName, sep=",", index=False)


################################################################################################
#   "PRIVATE" METHODS
################################################################################################
def mergeTemporaryCSVfiles(outputDir, totalUsers, numberOfReplicas):
	logger.info("merging temporary CSV files from: %s", outputDir)
	# mergeIterationCSVFiles(outputDir, totalUsers)
	# mergeResultCSVFiles(outputDir, totalUsers, numberOfReplicas)

def mergeResultCSVFiles(outputDir, totalUsers, numberOfReplicas):
	tempCSVFiles = glob.glob(outputDir + "/*.results.temp")
	logger.info("merging files: %s", tempCSVFiles)

	frame = pd.DataFrame()
	list_ = []
	for file_ in tempCSVFiles:
		df = pd.read_csv(file_, index_col=None, header=0)
		list_.append(df)

	frame = pd.concat(list_)
	# CSV format: numberOps,opsPerSecond,avgLatency,numberOfReplicas,usersNumber,usersPerEmulator
	totalCommits = frame['committed'].sum()
	tpmc = frame['tpmc'].sum()

	avgLatency = frame['avgLatency'].mean()
	aborted = frame['abortrate'].mean()

	opsPerSecond = totalCommits / config.TPCC_TEST_TIME
	usersPerEmulator = totalUsers / numberOfReplicas
	fileContent = "commits,opsPerSecond,avgLatency,numberOfReplicas,usersNumber,usersPerEmulator,aborted,tpmc\n"
	fileContent += str(totalCommits) + "," + str(opsPerSecond) + "," + str(avgLatency) + "," + str(
		numberOfReplicas) + "," + str(totalUsers) + "," + str(usersPerEmulator) + "," + str(aborted) + "," + str(tpmc)

	fileName = outputDir + "/" + str(totalUsers) + config.ACTIVE_EXPERIMENT + "_"
	fileName += config.JDBC
	fileName += ".csv"

	logger.info("creating csv file: %s", fileName)
	f = open(fileName, 'w')
	f.write(fileContent)
	f.close()


if __name__ == "__main__":
	(options, args) = parser.parse_args()

	#mandatories = ['outputDir', 'graphic']
	#for m in mandatories:
	#	if not options.__dict__[m]:
	#		print "mandatory option is missing\n"
	#		parser.print_help()
	#		exit(-1)

	outputDir = options.__dict__['outputDir']
	graphicType = int(options.__dict__['graphic'])

	if graphicType == 1:
		logger.info("plotting graphic {} with data from {}".format("Latency-Throughput", outputDir))
		plotLatencyThroughputGraphic(outputDir)
	elif graphicType == 2:
		logger.warn("missing plot generation")
		sys.exit(1)
	else:
		logger.error("unknow graphic type. Aborting")
		sys.exit(-1)
