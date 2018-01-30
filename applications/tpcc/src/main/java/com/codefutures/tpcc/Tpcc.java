package com.codefutures.tpcc;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.codefutures.tpcc.stats.PerSecondStatistics;
import com.codefutures.tpcc.stats.PerformanceCounters;
import com.codefutures.tpcc.stats.ThreadStatistics;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import weaql.common.nodes.NodeConfig;
import weaql.common.util.DatabaseProperties;
import weaql.common.util.Topology;


public class Tpcc implements TpccConstants
{

	private static final Logger logger = LoggerFactory.getLogger(Tpcc.class);
	private static final boolean DEBUG = logger.isDebugEnabled();

	public static final String VERSION = "1.0.1";
	private static final String DRIVER = "DRIVER";
	private static final String WAREHOUSECOUNT = "WAREHOUSECOUNT";
	private static final String USER = "USER";
	private static final String PASSWORD = "PASSWORD";
	private static final String CONNECTIONS = "CONNECTIONS";
	private static final String RAMPUPTIME = "RAMPUPTIME";
	private static final String DURATION = "DURATION";
	private static final String JDBCURL = "JDBCURL";
	private static final String JOINS = "JOINS";
	public static boolean CUSTOM_JDBC;
	private int proxyId;

	private final List<TpccThread> clientThreads;
	private List<PerformanceCounters> performanceCounters = new LinkedList<>();
	private final List<PerSecondStatistics> perSecondStatsList;

	private static final String PROPERTIESFILE = "client.database.properties";


    /* Global SQL Variables */

	private String javaDriver;
	private String jdbcUrl;
	private String dbUser;
	private String dbPassword;
	private Boolean joins = true;

	private int numWare;
	private int numConn;
	private int rampupTime;
	private int measureTime;
	private int fetchSize = 100;

	private int num_node; /* number of servers that consists of cluster i.e. RAC (0:normal mode)*/
	private static final String TRANSACTION_NAME[] = {"NewOrder", "Payment", "Order Stat", "Delivery", "Slev"};

	private final int[] success = new int[TRANSACTION_COUNT];
	private final int[] late = new int[TRANSACTION_COUNT];
	private final int[] retry = new int[TRANSACTION_COUNT];
	private final int[] failure = new int[TRANSACTION_COUNT];

	private int[][] success2;
	private int[][] late2;
	private int[][] retry2;
	private int[][] failure2;
	private double[] latencies;
	public static volatile boolean counting_on = false;

	private int[] success2_sum = new int[TRANSACTION_COUNT];
	private int[] late2_sum = new int[TRANSACTION_COUNT];
	private int[] retry2_sum = new int[TRANSACTION_COUNT];
	private int[] failure2_sum = new int[TRANSACTION_COUNT];

	private int[] prev_s = new int[5];
	private int[] prev_l = new int[5];

	private double[] max_rt = new double[5];
	private int port = 3306;

	private Properties properties;
	private InputStream inputStream;

	public static volatile int activate_transaction = 0;

	public Tpcc()
	{
		this.clientThreads = new ArrayList<>();
		this.perSecondStatsList = new ArrayList<>();
	}

	private void init()
	{
		logger.info("Loading properties from: " + PROPERTIESFILE);

		properties = new Properties();
		try
		{
			inputStream = getClass().getClassLoader().getResourceAsStream(PROPERTIESFILE);
			//inputStream = new FileInputStream(PROPERTIESFILE);
			properties.load(inputStream);
		} catch(IOException e)
		{
			throw new RuntimeException("Error loading properties file", e);
		}

	}

	private int runBenchmark(boolean overridePropertiesFile, String[] argv)
	{
		System.out.println("***************************************");
		System.out.println("****** Java TPC-C Load Generator ******");
		System.out.println("***************************************");

        /* initialize */
		RtHist.histInit();
		activate_transaction = 1;

		for(int i = 0; i < TRANSACTION_COUNT; i++)
		{
			success[i] = 0;
			late[i] = 0;
			retry[i] = 0;
			failure[i] = 0;

			prev_s[i] = 0;
			prev_l[i] = 0;

			max_rt[i] = 0.0;
		}

        /* number of node (default 0) */
		num_node = 0;

		if(overridePropertiesFile)
		{
			for(int i = 0; i < argv.length; i = i + 2)
			{
				if(argv[i].equals("-u"))
				{
					dbUser = argv[i + 1];
				} else if(argv[i].equals("-p"))
				{
					dbPassword = argv[i + 1];
				} else if(argv[i].equals("-w"))
				{
					numWare = Integer.parseInt(argv[i + 1]);
				} else if(argv[i].equals("-c"))
				{
					numConn = Integer.parseInt(argv[i + 1]);
				} else if(argv[i].equals("-r"))
				{
					rampupTime = Integer.parseInt(argv[i + 1]);
				} else if(argv[i].equals("-t"))
				{
					measureTime = Integer.parseInt(argv[i + 1]);
				} else if(argv[i].equals("-j"))
				{
					javaDriver = argv[i + 1];
				} else if(argv[i].equals("-l"))
				{
					jdbcUrl = argv[i + 1];
				} else if(argv[i].equals("-f"))
				{
					fetchSize = Integer.parseInt(argv[i + 1]);
				} else if(argv[i].equals("-J"))
				{
					joins = Boolean.parseBoolean(argv[i + 1]);
				} else
				{
					System.out.println("Incorrect Argument: " + argv[i]);
					System.out.println("The possible arguments are as follows: ");
					System.out.println("-h [database host]");
					System.out.println("-d [database name]");
					System.out.println("-u [database username]");
					System.out.println("-p [database password]");
					System.out.println("-w [number of warehouses]");
					System.out.println("-c [number of connections]");
					System.out.println("-r [ramp up time]");
					System.out.println("-t [duration of the benchmark (sec)]");
					System.out.println("-j [java driver]");
					System.out.println("-l [jdbc url]");
					System.out.println("-h [jdbc fetch size]");
					System.out.println("-J [joins (true|false) default true]");
					System.exit(-1);

				}
			}
		} else
		{

			dbUser = properties.getProperty(USER);
			dbPassword = properties.getProperty(PASSWORD);
			numWare = Integer.parseInt(properties.getProperty(WAREHOUSECOUNT));
			numConn = Integer.parseInt(properties.getProperty(CONNECTIONS));
			rampupTime = Integer.parseInt(properties.getProperty(RAMPUPTIME));
			measureTime = Integer.parseInt(properties.getProperty(DURATION));
			javaDriver = properties.getProperty(DRIVER);
			jdbcUrl = properties.getProperty(JDBCURL);
			String jdbcFetchSize = properties.getProperty("JDBCFETCHSIZE");
			joins = Boolean.parseBoolean(properties.getProperty(JOINS));

			if(jdbcFetchSize != null)
			{
				fetchSize = Integer.parseInt(jdbcFetchSize);
			}

		}

		int proxyId = Integer.parseInt(System.getProperty("proxyid"));
		this.proxyId = proxyId;

		NodeConfig nodeConfig = Topology.getInstance().getProxyConfigWithIndex(proxyId);
		boolean isCustomJDBC = Boolean.parseBoolean(System.getProperty("customJDBC"));
		DatabaseProperties dbProperties = nodeConfig.getDbProps();

		numConn = Integer.parseInt(System.getProperty("usersNum"));
		measureTime = Integer.parseInt(System.getProperty("testDuration"));

		dbUser = dbProperties.getDbUser();
		dbPassword = dbProperties.getDbPwd();
		jdbcUrl = dbProperties.getUrl();
		if(isCustomJDBC)
			javaDriver = "database.jdbc.CRDTDriver";
		else
			javaDriver = "com.mysql.jdbc.Driver";

		if(num_node > 0)
		{
			if(numWare % num_node != 0)
			{
				logger.error(" [warehouse] value must be devided by [num_node].");
				return 1;
			}
			if(numConn % num_node != 0)
			{
				logger.error("[connection] value must be devided by [num_node].");
				return 1;
			}
		}

		if(javaDriver == null)
		{
			throw new RuntimeException("Java Driver is null.");
		}
		if(jdbcUrl == null)
		{
			throw new RuntimeException("JDBC Url is null.");
		}
		if(dbUser == null)
		{
			throw new RuntimeException("User is null.");
		}
		if(dbPassword == null)
		{
			throw new RuntimeException("Password is null.");
		}
		if(numWare < 1)
		{
			throw new RuntimeException("Warehouse count has to be greater than or equal to 1.");
		}
		if(numConn < 1)
		{
			throw new RuntimeException("Connections has to be greater than or equal to 1.");
		}
		if(rampupTime < 1)
		{
			throw new RuntimeException("Rampup time has to be greater than or equal to 1.");
		}
		if(measureTime < 1)
		{
			throw new RuntimeException("Duration has to be greater than or equal to 1.");
		}

		// Init 2-dimensional arrays.
		success2 = new int[TRANSACTION_COUNT][numConn];
		late2 = new int[TRANSACTION_COUNT][numConn];
		retry2 = new int[TRANSACTION_COUNT][numConn];
		failure2 = new int[TRANSACTION_COUNT][numConn];
		latencies = new double[numConn];

		//long delay1 = measure_time*1000;

		System.out.printf("<Parameters>\n");

		System.out.printf("     [driver]: %s\n", javaDriver);
		System.out.printf("        [URL]: %s\n", jdbcUrl);
		System.out.printf("       [user]: %s\n", dbUser);
		System.out.printf("       [pass]: %s\n", dbPassword);
		System.out.printf("      [joins]: %b\n", joins);

		System.out.printf("  [warehouse]: %d\n", numWare);
		System.out.printf(" [connection]: %d\n", numConn);
		System.out.printf("     [rampup]: %d (sec.)\n", rampupTime);
		System.out.printf("    [measure]: %d (sec.)\n", measureTime);

		Util.seqInit(10, 10, 1, 1, 1);

        /* set up threads */
                //System.out.println("****In Tpcc before newFixedThreadPool numConn: "+numConn);
		if(DEBUG)
			logger.debug("Creating TpccThread");
		ExecutorService executor = Executors.newFixedThreadPool(numConn, new NamedThreadFactory("tpcc-thread"));

		// Start each server.
		//counting_on = true;
		for(int i = 0; i < numConn; i++)
		{
                        //System.out.println("****In runBenchmark before for loop of Tpcc class before call to Runnable TpccThread");
			Runnable worker = new TpccThread(i, port, 1, dbUser, dbPassword, numWare, numConn, javaDriver, jdbcUrl,
					fetchSize, success, late, retry, failure, success2, late2, retry2, failure2, latencies, joins,
					dbProperties);
			executor.execute(worker);
			this.clientThreads.add((TpccThread) worker);
		}

		if(rampupTime > 0)
		{
			// rampup time
			System.out.println("\nRAMPUP START.\n\n");
			try
			{
				Thread.sleep(rampupTime * 1000);
			} catch(InterruptedException e)
			{
				logger.error("Rampup wait interrupted", e);
			}
			System.out.println("\nRAMPUP END.\n\n");
		}

		// start counting
		counting_on = true;

		// loop for the measure_time
		final long startTime = System.currentTimeMillis();
		DecimalFormat df = new DecimalFormat("#,##0.0");
		long runTime = 0;
		int z = 0;
		while((runTime = System.currentTimeMillis() - startTime) < measureTime * 1000)
		{
			System.out.println("Current execution time lapse: " + df.format(runTime / 1000.0f) + " seconds");
			try
			{
				Thread.sleep(1000);
				logger.info("collecting statistics {}", z);
				this.collectStatistics(z);
				z++;
			} catch(InterruptedException e)
			{
				logger.error("Sleep interrupted", e);
			}
		}
		final long actualTestTime = System.currentTimeMillis() - startTime;
		counting_on = false;

		// show results
		System.out.println("---------------------------------------------------");
		/*
		 *  Raw Results
         */

		System.out.println("<Raw Results>");
		for(int i = 0; i < TRANSACTION_COUNT; i++)
		{
			System.out.printf("  |%s| sc:%d  lt:%d  rt:%d  fl:%d \n", TRANSACTION_NAME[i], success[i], late[i],
					retry[i], failure[i]);
		}

		System.out.printf(" in %f sec.\n", actualTestTime / 1000.0f);

        /*
		* Raw Results 2
        */
		System.out.println("<Raw Results2(sum ver.)>");
		for(int i = 0; i < TRANSACTION_COUNT; i++)
		{
			success2_sum[i] = 0;
			late2_sum[i] = 0;
			retry2_sum[i] = 0;
			failure2_sum[i] = 0;
			for(int k = 0; k < numConn; k++)
			{
				success2_sum[i] += success2[i][k];
				late2_sum[i] += late2[i][k];
				retry2_sum[i] += retry2[i][k];
				failure2_sum[i] += failure2[i][k];
			}
		}
		for(int i = 0; i < TRANSACTION_COUNT; i++)
		{
			System.out.printf("  |%s| sc:%d  lt:%d  rt:%d  fl:%d \n", TRANSACTION_NAME[i], success2_sum[i],
					late2_sum[i], retry2_sum[i], failure2_sum[i]);
		}

		System.out.println("<Constraint Check> (all must be [OK])\n [transaction percentage]");
		int j = 0;
		int i;
		for(i = 0; i < TRANSACTION_COUNT; i++)
		{
			j += (success[i] + late[i]);
		}

		double f = 100.0 * (float) (success[1] + late[1]) / (float) j;
		System.out.printf("        Payment: %f%% (>=43.0%%)", f);
		if(f >= 43.0)
		{
			System.out.printf(" [OK]\n");
		} else
		{
			System.out.printf(" [NG] *\n");
		}
		f = 100.0 * (float) (success[2] + late[2]) / (float) j;
		System.out.printf("   Order-Status: %f%% (>= 4.0%%)", f);
		if(f >= 4.0)
		{
			System.out.printf(" [OK]\n");
		} else
		{
			System.out.printf(" [NG] *\n");
		}
		f = 100.0 * (float) (success[3] + late[3]) / (float) j;
		System.out.printf("       Delivery: %f%% (>= 4.0%%)", f);
		if(f >= 4.0)
		{
			System.out.printf(" [OK]\n");
		} else
		{
			System.out.printf(" [NG] *\n");
		}
		f = 100.0 * (float) (success[4] + late[4]) / (float) j;
		System.out.printf("    Stock-Level: %f%% (>= 4.0%%)", f);
		if(f >= 4.0)
		{
			System.out.printf(" [OK]\n");
		} else
		{
			System.out.printf(" [NG] *\n");
		}

        /*
        * Response Time
        */
		System.out.printf(" [response time (at least 90%% passed)]\n");

		for(int n = 0; n < TRANSACTION_NAME.length; n++)
		{
			f = 100.0 * (float) success[n] / (float) (success[n] + late[n]);
			if(DEBUG)
				logger.debug("f: " + f + " success[" + n + "]: " + success[n] + " late[" + n + "]: " + late[n]);
			System.out.printf("      %s: %f%% ", TRANSACTION_NAME[n], f);
			if(f >= 90.0)
			{
				System.out.printf(" [OK]\n");
			} else
			{
				System.out.printf(" [NG] *\n");
			}
		}

		double total = 0.0;
		for(j = 0; j < TRANSACTION_COUNT; j++)
		{
			total = total + success[j] + late[j];
			System.out.println(" " + TRANSACTION_NAME[j] + " Total: " + (success[j] + late[j]));
		}

		//float tpcm = (success[0] + late[0]) * 60000f / actualTestTime;
		//TPMC = tpcm;
		int newOrderCommits = 0;
		for(int k = 0; k < numConn; k++)
		{
			newOrderCommits += success2[0][k];
			newOrderCommits += late2[0][k];
		}

		TPMC = newOrderCommits * 60000f / actualTestTime;

		System.out.println();
		System.out.println("<TpmC>");
		System.out.println(TPMC + " TpmC");

		// stop threads
		System.out.printf("\nSTOPPING THREADS\n");
		activate_transaction = 0;
		executor.shutdown();

		try
		{
			executor.awaitTermination(30, TimeUnit.SECONDS);
		} catch(InterruptedException e)
		{
			System.out.println("Timed out waiting for executor to terminate");
		}

		//TODO: To be implemented better later.
		//RtHist.histReport();
		return 0;

	}

	public static void main(String[] argv)
	{

		if(argv.length != 5)
		{
			logger.error("usage: java -jar <config_file_path> <proxyId> <num_users> <useCustomJDBC> <testDuration>");
			System.exit(1);
		}
		String configFile = argv[0];
		int proxyId = Integer.parseInt(argv[1]);
		int usersNum = Integer.parseInt(argv[2]);
		boolean useCustomJDBC = Boolean.parseBoolean(argv[3]);
		int testDuration = Integer.parseInt(argv[4]);

		//System.setProperty("configPath", configFile);
		System.setProperty("proxyid", String.valueOf(proxyId));

		//TODO: not compiling atm
		//Configuration.setupConfiguration(configFile);
		System.setProperty("usersNum", String.valueOf(usersNum));
		System.setProperty("customJDBC", String.valueOf(useCustomJDBC));
		System.setProperty("testDuration", String.valueOf(testDuration));

		CUSTOM_JDBC = useCustomJDBC;

		System.out.println("TPCC version " + VERSION + " Number of Arguments: " + argv.length);

		// dump information about the environment we are running in
		String sysProp[] = {"os.name", "os.arch", "os.version", "java.runtime.name", "java.vm.version", "java.library"
				+ ".path"};

		for(String s : sysProp)
		{
			logger.info("System Property: " + s + " = " + System.getProperty(s));
		}

		DecimalFormat df = new DecimalFormat("#,##0.0");
		System.out.println("maxMemory = " + df.format(Runtime.getRuntime().totalMemory() / (1024.0 * 1024.0)) + " MB");

		Tpcc tpcc = new Tpcc();

		int ret = 0;

		if(argv.length == 5)
		{
			tpcc.init();
			ret = tpcc.runBenchmark(false, argv);
		} else if(argv.length == 0)
		{

			System.out.println("Using the properties file for configuration.");
			tpcc.init();
			ret = tpcc.runBenchmark(false, argv);

		} else
		{
			if((argv.length % 2) == 0)
			{
				System.out.println("Using the command line arguments for configuration.");
				ret = tpcc.runBenchmark(true, argv);
			} else
			{
				System.out.println("Invalid number of arguments.");
				System.out.println("The possible arguments are as follows: ");
				System.out.println("-h [database host]");
				System.out.println("-d [database name]");
				System.out.println("-u [database username]");
				System.out.println("-p [database password]");
				System.out.println("-w [number of warehouses]");
				System.out.println("-c [number of connections]");
				System.out.println("-r [ramp up time]");
				System.out.println("-t [duration of the benchmark (sec)]");
				System.out.println("-j [java driver]");
				System.out.println("-l [jdbc url]");
				System.out.println("-h [jdbc fetch size]");
				System.exit(-1);
			}

		}
		//tpcc.mergeCounters();
		tpcc.createOutputFiles();
		tpcc.createIterationsFile();
		System.out.println("--------------------------- SUMMARY ---------------------------");
		System.out.println("Abort rate:" + ABORT_RATE);
		System.out.println("Average latency:" + AVG_LATENCY);
		System.out.println("Commit Counter:" + COMMITS);
		System.out.println("Measured tpmC:" + TPMC);
		System.out.println("---------------------------------------------------------------");
		System.out.println("Terminating process now");
		System.out.println("CLIENT TERMINATED");
		System.exit(ret);
	}

	public void createOutputFiles()
	{
		/*
		int commitsCounter = COMMITS;
		double avgLatency = AVG_LATENCY;
		float abortRate = ABORT_RATE;
		float tpmc = TPMC;   */

		int commitsCounter = 0;
		float totalLatency = 0.0f;

		for(int i = 0; i < this.success2_sum.length; i++)
			commitsCounter += success2_sum[i];

		for(int i = 0; i < this.late2_sum.length; i++)
			commitsCounter += late2_sum[i];

		for(int i = 0; i < this.latencies.length; i++)
			totalLatency += this.latencies[i];

		float avgLatency = totalLatency / commitsCounter;

		float abortCounter = 0;

		for(int i = 0; i < this.failure2_sum.length; i++)
			abortCounter += this.failure2_sum[i];

		float abortRate = abortCounter * 1.0f / (abortCounter + commitsCounter);

		COMMITS = commitsCounter;
		AVG_LATENCY = avgLatency;
		ABORT_RATE = abortRate;

		String fileName = "emulator" + proxyId + ".results.temp";

		// OPS LATENCY CLIENTS

		PrintWriter out = null;
		try
		{
			StringBuilder buffer = new StringBuilder();
			buffer.append("committed,avgLatency,tpmc,abortrate\n");
			buffer.append(COMMITS);
			buffer.append(",");
			buffer.append(AVG_LATENCY);
			buffer.append(",");
			buffer.append(TPMC);
			buffer.append(",");
			buffer.append(abortRate);
			out = new PrintWriter(fileName);
			out.write(buffer.toString());
			out.close();
		} catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	public void createIterationsFile()
	{
		this.finalizeStats();

		// iteration files
		// CSV style: iteration,success,aborts,avgLatency,maxLatency,minLatency
		String fileName = "emulator" + this.proxyId + ".iters.temp";
		StringBuffer buffer = new StringBuffer();

		buffer.append("emulatorid,iteration,success,aborts,avgLatency,maxLatency,minLatency\n");

		for(PerSecondStatistics perSecondStats : this.perSecondStatsList)
		{
			buffer.append(this.proxyId);
			buffer.append(",");
			buffer.append(perSecondStats.toString());
			buffer.append("\n");
		}

		PrintWriter out;
		try
		{
			out = new PrintWriter(fileName);
			out.write(buffer.toString());
			out.close();
		} catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	public void collectStatistics(int iteration)
	{
		PerSecondStatistics perSecondStat = new PerSecondStatistics(iteration);

		for(TpccThread thread : this.clientThreads)
		{
			ThreadStatistics t = new ThreadStatistics(thread.getStats());
			perSecondStat.addThreadStatistic(t);
		}

		this.perSecondStatsList.add(perSecondStat);
	}

	public void finalizeStats()
	{
		for(PerSecondStatistics perSecondStats : this.perSecondStatsList)
			perSecondStats.calculateStats();
	}

	public void printResults()
	{
		for(PerSecondStatistics perSecondStats : this.perSecondStatsList)
			logger.info(perSecondStats.toString());
	}

	private void mergeCounters()
	{
		int totalOps = 0;
		double avgLatency = 0;
		float abortRate = 0.0f;
		float tpmc = 0.0f;

		for(TpccThread thread : this.clientThreads)
			this.performanceCounters.add(thread.getPerformanceCounter());

		for(PerformanceCounters counter : this.performanceCounters)
			totalOps += counter.getCommitCounter();

		COMMITS = totalOps;

		for(PerformanceCounters counter : this.performanceCounters)
			avgLatency += counter.getAverageLatency() * counter.getCommitCounter();

		avgLatency = avgLatency / totalOps;
		AVG_LATENCY = avgLatency;

		for(PerformanceCounters counter : this.performanceCounters)
			abortRate += counter.getTotalAbortRate() * counter.getCommitCounter();

		abortRate = abortRate / totalOps;
		ABORT_RATE = abortRate;

		for(PerformanceCounters counter : this.performanceCounters)
			tpmc += counter.getTotalNewOrderCommitRate() * counter.getCommitCounter();

		tpmc = tpmc / totalOps;
		TPMC = tpmc;
	}

	public static int COMMITS = 0;
	public static double AVG_LATENCY = 0;
	public static float ABORT_RATE, TPMC = 0.0f;

}

