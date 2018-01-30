package weaql.server.replicator;


import org.apache.commons.dbutils.DbUtils;
import weaql.common.util.*;
import weaql.common.util.defaults.ScratchpadDefaults;
import weaql.common.util.exception.InitComponentFailureException;
import weaql.common.util.exception.InvalidConfigurationException;
import weaql.server.agents.AgentsFactory;
import weaql.server.util.StatsCollector;
import weaql.server.execution.DBCommitterAgent;
import weaql.server.execution.DBCommitter;
import weaql.common.nodes.AbstractNode;
import weaql.common.nodes.NodeConfig;
import weaql.server.agents.coordination.SimpleCoordinationAgent;
import weaql.server.agents.coordination.CoordinationAgent;
import weaql.server.agents.deliver.DeliverAgent;
import weaql.server.agents.dispatcher.DispatcherAgent;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.util.LogicalClock;
import weaql.common.util.defaults.ReplicatorDefaults;
import weaql.common.thrift.*;
import weaql.server.util.TransactionCommitFailureException;

import java.sql.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by dnlopes on 15/03/15.
 */
public class Replicator extends AbstractNode
{
	
	private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);
	private static final String SUBPREFIX = "r";

	private LogicalClock clock;
	private final IReplicatorNetwork networkInterface;
	private final ObjectPool<DBCommitter> agentsPool;
	private final Lock clockLock;
	private final String prefix;
	private final AtomicInteger txnCounter;
	public static AtomicInteger abortCounter;
	protected final StatsCollector statsCollector;

	private final GarbageCollector garbageCollector;
	private final ScheduledExecutorService scheduleService;
	private final DeliverAgent deliver;
	private final DispatcherAgent dispatcher;
	private final CoordinationAgent coordAgent;

	public Replicator(NodeConfig config) throws InitComponentFailureException, InvalidConfigurationException
	{
		super(config);

		this.prefix = SUBPREFIX + this.config.getId() + "_";
		this.txnCounter = new AtomicInteger();
		abortCounter = new AtomicInteger();
		this.networkInterface = new ReplicatorNetwork(config);

		this.deliver = AgentsFactory.createDeliverAgent(this);
		this.dispatcher = AgentsFactory.createDispatcherAgent(this);
		this.coordAgent = new SimpleCoordinationAgent(this);

		this.clock = new LogicalClock(Topology.getInstance().getReplicatorsCount());
		this.agentsPool = new ObjectPool<>();
		this.clockLock = new ReentrantLock();
		this.statsCollector = new StatsCollector();

		this.scheduleService = Executors.newScheduledThreadPool(2);
		this.garbageCollector = new GarbageCollector(this);
		this.scheduleService.scheduleAtFixedRate(garbageCollector, 0,
				ReplicatorDefaults.GARBAGE_COLLECTOR_THREAD_INTERVAL, TimeUnit.MILLISECONDS);
		this.scheduleService.scheduleAtFixedRate(new StateChecker(),
				ReplicatorDefaults.STATE_CHECKER_THREAD_INTERVAL * 4, ReplicatorDefaults.STATE_CHECKER_THREAD_INTERVAL,
				TimeUnit.MILLISECONDS);

		deleteScratchpads();
		createCommiterAgents();

		try
		{
			new Thread(new ReceiverThread(this)).start();
		} catch(TTransportException e)
		{
			String error = "failed to create background thread for replicator: " + e.getMessage();
			throw new InitComponentFailureException(error);
		}

		networkInterface.openConnections();
		LOG.info("replicator " + this.config.getId() + " online");
	}

	/**
	 * Attempts to commit a transaction.
	 *
	 * @param txn
	 *
	 * @return true if it was sucessfully committed locally, false otherwise
	 */
	public Status commitOperation(CRDTCompiledTransaction txn, boolean shouldMergeClocks)
			throws TransactionCommitFailureException
	{
		if(shouldMergeClocks)
			mergeWithRemoteClock(new LogicalClock(txn.getTxnClock()));

		DBCommitter pad = this.agentsPool.borrowObject();

		if(pad == null)
		{
			LOG.warn("commit pad pool was empty. creating new one");
			try
			{
				pad = new DBCommitterAgent(this.config);
			} catch(SQLException e)
			{
				LOG.warn("failed to create new commitpad at runtime", e);
			}

			if(pad == null)
			{
				LOG.warn("failed to create commitpad on the fly. will be looping until get one");
				pad = closedLoopGetPad();
			}
		}

		LOG.trace("txn ({}) from replicator {} committing on main storage ", txn.getTxnClock(), txn.getReplicatorId());

		Status status = pad.commitTrx(txn);

		if(!status.isSuccess())
			LOG.error(status.getError());

		this.agentsPool.returnObject(pad);

		return status;
	}

	public IReplicatorNetwork getNetworkInterface()
	{
		return this.networkInterface;
	}

	public LogicalClock getNextClock()
	{
		this.clockLock.lock();

		this.clock.increment(this.config.getId() - 1);
		LogicalClock newClock = new LogicalClock(this.clock.getDcEntries());

		this.clockLock.unlock();

		return newClock;
	}

	public String getPrefix()
	{
		return this.prefix;
	}

	public LogicalClock getCurrentClock()
	{
		return this.clock;
	}

	public DispatcherAgent getDispatcher()
	{
		return this.dispatcher;
	}

	public DeliverAgent getDeliver()
	{
		return this.deliver;
	}

	public CoordinationAgent getCoordAgent()
	{
		return this.coordAgent;
	}

	private void mergeWithRemoteClock(LogicalClock clock)
	{
		LOG.trace("merging clocks {} with {}", this.clock.toString(), clock.toString());

		this.clockLock.lock();
		this.clock = this.clock.maxClock(clock);
		this.clockLock.unlock();
	}

	private DBCommitter closedLoopGetPad()
	{
		int counter = 0;
		DBCommitter pad;

		do
		{
			pad = this.agentsPool.borrowObject();
			counter++;

			if(counter % 150 == 0)
			{
				LOG.warn("already tried {} to get commit pad from pool", counter);
				counter = 0;
			}

		} while(pad == null);

		return pad;
	}

	private void createCommiterAgents()
	{
		int agentsNumber = WeaQLEnvironment.COMMIT_PAD_POOL_SIZE;

		for(int i = 0; i < agentsNumber; i++)
		{
			try
			{
				DBCommitter agent = new DBCommitterAgent(getConfig());

				if(agent != null)
					this.agentsPool.addObject(agent);

			} catch(SQLException e)
			{
				LOG.warn(e.getMessage(), e);
			}
		}

		LOG.info("{} commit agents available for main storage execution", this.agentsPool.getPoolSize());
	}

	private void deleteScratchpads()
	{
		Connection con = null;
		Statement stat = null;

		try
		{
			con = ConnectionFactory.getDefaultConnection(config);
			con.setAutoCommit(false);
			DatabaseMetaData metadata = con.getMetaData();
			stat = con.createStatement();
			ResultSet rs = metadata.getTables(null, null, "%", null);

			while(rs.next())
			{
				String tableName = rs.getString(3);

				if(tableName.startsWith(ScratchpadDefaults.SCRATCHPAD_TABLE_ALIAS_PREFIX))
					stat.execute("DROP TABLE " + tableName);
			}

			con.commit();

		} catch(SQLException e)
		{
			LOG.warn("failed to cleanup temporary tables: {}", e.getMessage());
		} finally
		{
			DbUtils.closeQuietly(stat);
			DbUtils.closeQuietly(con);
		}
	}

	public int assignNewTransactionId()
	{
		return this.txnCounter.incrementAndGet();
	}

	private class StateChecker implements Runnable
	{

		private int id = config.getId();

		@Override
		public void run()
		{
			long[] entries = clock.getDcEntries();
			long opsSum = 0;
			for(int i = 0; i < entries.length; i++)
				opsSum += entries[i];

			int percentage = (int) (abortCounter.get() * 100.0 / opsSum + 0.5);

			LOG.info("<r{}> vector clock: ({})", id, clock.getClockValue());
			//LOG.info("Total ops: {} ; Total aborts: {} ; Abort rate: {}%", opsSum, abortCounter.get(), percentage);
		}
	}
}
