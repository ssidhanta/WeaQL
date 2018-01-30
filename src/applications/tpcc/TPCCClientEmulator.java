package applications.tpcc;


import applications.BaseBenchmarkOptions;
import applications.BenchmarkOptions;
import applications.Transaction;
import applications.util.TransactionRecord;
import weaql.client.jdbc.CRDTConnectionFactory;
import weaql.common.util.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import weaql.common.util.Topology;
import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.exception.ConfigurationLoadException;
//import weaql.client.proxy.SandboxExecutionProxy;


/**
 * Created by dnlopes on 05/06/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public class TPCCClientEmulator implements Runnable
{

	private static final Logger LOG = LoggerFactory.getLogger(TPCCClientEmulator.class);

	private Connection connection;
	private int id;
	private final BaseBenchmarkOptions options;

	private TPCCStatistics stats;
	private long execLatency, commitLatency;
	private int benchmarkDuration;
	//Added by Subhajit
        
        private String topologyFile;
        private String envFile;
        private String workloadFile;
	//private SandboxExecutionProxy proxy;

	public TPCCClientEmulator(int id, BaseBenchmarkOptions options, String topologyFile, String envFile, String workloadFile) //, SandboxExecutionProxy proxy)
	{
                this.id = id;
                    this.options = options;
                    this.stats = new TPCCStatistics(this.id);
                    this.envFile = envFile;
                    this.workloadFile = workloadFile;
                    this.topologyFile = topologyFile;
		try
		{
                    
                    Topology.setupTopology(this.topologyFile);
                    WeaQLEnvironment.setupEnvironment(this.envFile);
                    TpccBenchmark.loadWorkloadFile(this.workloadFile);
                    try
                    {
                        if(this.options.isCRDTDriver())
                            this.connection = CRDTConnectionFactory.getCRDTConnection(this.options.getDbProps(),
                                    this.options.getDatabaseName());
                        else
                            this.connection = ConnectionFactory.getDefaultConnection(this.options.getDbProps(),
                                    this.options.getDatabaseName());
                    } catch(SQLException | ClassNotFoundException e)
                    {
                        LOG.error("failed to create connection for client: {}", e.getMessage(), e);
                        System.exit(-1);
                    }
                    this.benchmarkDuration = this.options.getDuration();
                    //Added by Subhajit
                    //this.proxy = proxy;
		} catch(ConfigurationLoadException ex)
		{
			java.util.logging.Logger.getLogger(TPCCClientEmulator.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@Override
	public void run()
	{
		long startTime = System.currentTimeMillis();
		long rampUpTime;

		LOG.info("client {} started ramping up", id);

		while((rampUpTime = System.currentTimeMillis() - startTime) < BenchmarkOptions.Defaults.RAMPUP_TIME * 1000)
		{
			// RAMP UP TIME
			Transaction txn = this.options.getWorkload().getNextTransaction(options);
                        //System.out.println("after txn getworkload get next transaction: "+txn.getName());
			boolean success = false;

			while(!success)
			{
				success = tryTransaction(txn);
                                //System.out.println("*****In TPCCC Client EMulator transaction OKAY.");
				if(!success)
				{
					String error = txn.getLastError();
					//if((!error.contains("try restarting transaction")) && (!error.contains("Duplicate entry")))
						LOG.error(error);
				}
			}
		}

		LOG.info("client {} ended ramp up time and will start the actual experiment", id);

		startTime = System.currentTimeMillis();
		long benchmarkTime;

		System.out.println("starting actual experiment");

		while((benchmarkTime = System.currentTimeMillis() - startTime) < benchmarkDuration * 1000)
		{
			// benchmark time here
			Transaction txn = this.options.getWorkload().getNextTransaction(options);
			execLatency = 0;
			commitLatency = 0;
			boolean success = false;

			while(!success)
			{
				execLatency = 0;
				commitLatency = 0;
				success = tryTransaction(txn);

				if(success)
				{
                                        //.out.println("****+In TPCC Client Emulator inside If successs.");
					this.stats.addTxnRecord(txn.getName(),
							new TransactionRecord(txn.getName(), execLatency, commitLatency, true,
									TPCCEmulator.ITERATION));
				} else
				{
                                        //System.out.println("****+In TPCC Client Emulator inside Else successs.");
					String error = txn.getLastError();
					this.stats.addTxnRecord(txn.getName(),
							new TransactionRecord(txn.getName(), false, TPCCEmulator.ITERATION));

					if((!error.contains("try restarting transaction")) && (!error.contains("Duplicate entry")))
						LOG.error(error);
				}
			}
		}

		LOG.info("client {} finished the actual experiment", id);
	}

	private boolean tryTransaction(Transaction trx)
	{
		long beginExec = System.nanoTime();
                boolean commitSuccess = false;
		try{
                boolean execSuccess = trx.executeTransaction(this.connection);//, this.proxy);
		long execTime = System.nanoTime() - beginExec;
		execLatency += execTime;

		if(!execSuccess)
			return false;

		long beginCommit = System.nanoTime();
		commitSuccess = trx.commitTransaction(this.connection);//, this.proxy);
		long commitTime = System.nanoTime() - beginCommit;
		commitLatency += commitTime;
                }catch(Exception e){
                    commitSuccess = false;
                    e.printStackTrace();
                    return commitSuccess;
                }
		return commitSuccess;
	}

	public TPCCStatistics getStats()
	{
		return this.stats;
	}

}
