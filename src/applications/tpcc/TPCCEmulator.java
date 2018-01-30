package applications.tpcc;


import applications.BaseBenchmarkOptions;
import applications.BenchmarkOptions;
import weaql.common.util.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.exception.ConfigurationLoadException;
//import weaql.client.proxy.SandboxExecutionProxy;


/**
 * Created by dnlopes on 05/06/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public class TPCCEmulator
{

	private static final Logger LOG = LoggerFactory.getLogger(TPCCEmulator.class);

	public static volatile boolean RUNNING;
	public static volatile boolean COUTING;
	public static volatile int ITERATION = 0;

	public static int DURATION;
	private ExecutorService threadsService;
	private List<TPCCClientEmulator> clients;
	private Map<Integer, List<TPCCStatistics>> perSecondStats;
	private int emulatorId;
	private BaseBenchmarkOptions options;
	//Added by Subhajit
        private String topologyFile;
        private String envFile;
        private String workloadFile;
	//private SandboxExecutionProxy proxy;

	public TPCCEmulator(int id, BaseBenchmarkOptions options, String topologyFile, String envFile, String workloadFile)//, SandboxExecutionProxy proxy)
	{
		RUNNING = true;
		COUTING = false;
		this.emulatorId = id;
		this.options = options;
		DURATION = options.getDuration();
		this.clients = new ArrayList<>();
		this.threadsService = Executors.newFixedThreadPool(this.options.getClientsNumber());
		this.perSecondStats = new HashMap<>();
                this.envFile = envFile;
                this.workloadFile = workloadFile;
                this.topologyFile = topologyFile;
		//Added by Subhajit
		//this.proxy = proxy;
	}

	public boolean runBenchmark()
	{
                System.out.println("****************************************");
                System.out.print("************  ");
                System.out.print(this.options.getName());
                System.out.print("  ************");
                System.out.println();
                System.out.println("Number of clients: " + this.options.getClientsNumber());
                System.out.println("JDBC driver: " + this.options.getJdbc());
                System.out.println("Benchmark duration: " + this.options.getDuration());
                System.out.println("****************************************");
                
                for(int i = 0; i < this.options.getClientsNumber(); i++)
                {
                    TPCCClientEmulator client = new TPCCClientEmulator(i, this.options, this.topologyFile, this.envFile, this.workloadFile);//, this.proxy);
                    this.clients.add(client);
                    this.threadsService.execute(client);
                }
                
                if(BenchmarkOptions.Defaults.RAMPUP_TIME > 0)
                {
                    System.out.println("Starting ramp up time...");
                    try
                    {
                        Thread.sleep(BenchmarkOptions.Defaults.RAMPUP_TIME * 1000);
                    } catch(InterruptedException e)
                    {
                        LOG.error("ramp up time interrupted: {}", e.getMessage());
                        this.shutdownEmulator();
                        return false;
                    }
                    LOG.info("Ramp up time ended!");
                }
                
                final long startTime = System.currentTimeMillis();
                DecimalFormat df = new DecimalFormat("#,##0.0");
                long runTime;
                COUTING = true;
                
                int sleepDuration = options.getDuration() + 5;
                
                while((runTime = System.currentTimeMillis() - startTime) < sleepDuration * 1000)
                {
                    LOG.info("Current execution time lapse: " + df.format(runTime / 1000.0f) + " seconds");
                    try
                    {
                        Thread.sleep(5000);
                        ITERATION++;
                    } catch(InterruptedException e)
                    {
                        LOG.error("Benchmark interrupted: {}", e.getMessage());
                        this.shutdownEmulator();
                        return false;
                    }
                }
                
                RUNNING = false;
                COUTING = false;
                
                final long actualTestTime = System.currentTimeMillis() - startTime;
                
                LOG.info("Experiment ended!");
                LOG.info("Benchmark elapsed time: " + df.format(actualTestTime / 1000.0f));
                
                return true;
            
	}

	public void printStatistics()
	{
		TPCCStatistics globalStats = new TPCCStatistics(0);

		for(TPCCClientEmulator client : this.clients)
		{
			TPCCStatistics partialStats = client.getStats();
			globalStats.mergeStatistics(partialStats);
		}

		globalStats.generateStatistics();
		String statsString = globalStats.getStatsString();
		String distributionStrings = globalStats.getDistributionStrings();

		PrintWriter out;

		StringBuffer buffer = new StringBuffer();
		StringBuffer distributionBuffer = new StringBuffer();
		buffer.append("txn_name").append(",");
		buffer.append("commits").append(",");
		buffer.append("aborts").append(",");
		buffer.append("maxLatency").append(",");
		buffer.append("minLatency").append(",");
		buffer.append("avgLatency").append(",");
		buffer.append("avgExecLatency").append(",");
		buffer.append("avgCommitLatency").append(",");
		buffer.append("txnPerSecond").append("\n");
		buffer.append(statsString).append("\n");

		distributionBuffer.append(distributionStrings).append("\n");

		String outputContent = buffer.toString();
		String distributionContent = distributionBuffer.toString();

		try
		{
			String fileName = Topology.getInstance().getReplicatorsCount() + "_replicas_" + Topology.getInstance()
					.getEmulatorsCount() * options.getClientsNumber() + "_users_" + options.getJdbc() +
					"_jdbc_emulator" + this.emulatorId + ".csv";
			String distributionFileName = Topology.getInstance().getReplicatorsCount() + "_replicas_" + options
					.getClientsNumber() * Topology.getInstance().getEmulatorsCount() + "_users_" + options.getJdbc() +
					"_jdbc_emulator" + this.emulatorId +
					"_distribution.log";

			out = new PrintWriter(fileName);
			out.write(outputContent);
			out.close();
			out = new PrintWriter(distributionFileName);
			out.write(distributionContent);
			out.close();
		} catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	public void printStatistics3()
	{
		Map<Integer, TPCCStatistics> allSecondsStats = new HashMap<>();

		for(Map.Entry<Integer, List<TPCCStatistics>> aSecondStats : perSecondStats.entrySet())
		{
			TPCCStatistics aSecondStat = new TPCCStatistics(0);

			for(TPCCStatistics aStat : aSecondStats.getValue())
				aSecondStat.mergeStatistics(aStat);

			aSecondStat.generateStatistics();
			allSecondsStats.put(aSecondStats.getKey(), aSecondStat);
		}

		StringBuilder buffer = new StringBuilder();
		buffer.append("iteration").append(",");
		buffer.append("txn_name").append(",");
		buffer.append("commits").append(",");
		buffer.append("aborts").append(",");
		buffer.append("maxLatency").append(",");
		buffer.append("minLatency").append(",");
		buffer.append("avgLatency").append(",");
		buffer.append("avgExecLatency").append(",");
		buffer.append("avgCommitLatency").append("\n");

		for(Map.Entry<Integer, TPCCStatistics> aSecondStat : allSecondsStats.entrySet())
		{
			//buffer.append(aSecondStat.getKey()).append(",");
			buffer.append(aSecondStat.getValue().getStatsString(aSecondStat.getKey()));
			buffer.append("\n");
		}

		PrintWriter out;

		try
		{
			String fileName = Topology.getInstance().getReplicatorsCount() + "_replicas_" + options.getClientsNumber()
					* Topology.getInstance().getReplicatorsCount() + "_users_" + options.getJdbc() + "_jdbc_emulator"
					+ this.emulatorId + ".csv";

			out = new PrintWriter(fileName);
			out.write(buffer.toString());
			out.close();
		} catch(FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	public void shutdownEmulator()
	{
		COUTING = false;
		RUNNING = false;
		this.threadsService.shutdown();

		try
		{
			this.threadsService.awaitTermination(5000, TimeUnit.MILLISECONDS);
		} catch(InterruptedException e)
		{
			LOG.error(e.getMessage(), e);
		}
	}

}
