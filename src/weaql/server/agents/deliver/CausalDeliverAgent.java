package weaql.server.agents.deliver;


import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.Topology;
import weaql.common.util.defaults.ReplicatorDefaults;
import weaql.server.replicator.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.util.LogicalClock;
import weaql.server.util.TransactionCommitFailureException;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Created by dnlopes on 23/05/15.
 */
public class CausalDeliverAgent implements DeliverAgent
{

	private static final Logger LOG = LoggerFactory.getLogger(CausalDeliverAgent.class);

	private final Map<Integer, Queue<CRDTCompiledTransaction>> queues;
	private final Replicator replicator;
	private final ScheduledExecutorService scheduleService;

	public CausalDeliverAgent(Replicator replicator)
	{
		this.replicator = replicator;
		this.queues = new HashMap<>();

		//WeaQLEnvironment.REMOTE_APPLIER_THREAD_COUNT = Topology.getInstance().getReplicatorsCount();

		setup();

		this.scheduleService = Executors.newScheduledThreadPool(1 + WeaQLEnvironment.REMOTE_APPLIER_THREAD_COUNT);
		this.scheduleService.scheduleAtFixedRate(new StateChecker(),
				ReplicatorDefaults.STATE_CHECKER_THREAD_INTERVAL * 4, ReplicatorDefaults.STATE_CHECKER_THREAD_INTERVAL,
				TimeUnit.MILLISECONDS);

		for(int i = 0; i < WeaQLEnvironment.REMOTE_APPLIER_THREAD_COUNT; i++)
		{
			DeliveryThread deliveryThread = new DeliveryThread();
			this.scheduleService.scheduleAtFixedRate(deliveryThread, 0, THREAD_WAKEUP_INTERVAL, TimeUnit.MILLISECONDS);
		}
	}

	private void setup()
	{
		int replicatorsNumber = Topology.getInstance().getReplicatorsCount();

		for(int i = 0; i < replicatorsNumber; i++)
		{
			Queue q = new PriorityBlockingQueue(QUEUE_INITIAL_SIZE, new LogicalClockComparator(i));
			queues.put(i + 1, q); // i+1 because replicator id starts at 1 but clock index starts at 0
		}
	}

	@Override
	public void deliverTransaction(CRDTCompiledTransaction op) throws TransactionCommitFailureException
	{
		if(canDeliver(op))
			replicator.commitOperation(op, true);
		else
			addToQueue(op);
	}

	private void addToQueue(CRDTCompiledTransaction op)
	{
		int replicatorId = op.getReplicatorId();

		LOG.trace("adding op with clock ({}) to queue", op.getTxnClock());

		queues.get(replicatorId).add(op);
	}

	private boolean canDeliver(CRDTCompiledTransaction op)
	{
		LogicalClock opClock = new LogicalClock(op.getTxnClock());
		return replicator.getCurrentClock().lessThanByAtMostOne(opClock);
	}

	private class LogicalClockComparator implements Comparator<CRDTCompiledTransaction>
	{

		private final int index;

		public LogicalClockComparator(int index)
		{
			this.index = index;
		}

		@Override
		public int compare(CRDTCompiledTransaction transaction1, CRDTCompiledTransaction transaction2)
		{
			LogicalClock clock1 = new LogicalClock(transaction1.getTxnClock());
			LogicalClock clock2 = new LogicalClock(transaction2.getTxnClock());
			long entry1 = clock1.getEntry(this.index);
			long entry2 = clock2.getEntry(this.index);

			if(entry1 == entry2)
				return 0;
			if(entry1 > entry2)
				return 1;
			else
				return -1;
		}
	}


	private class StateChecker implements Runnable
	{

		private int id = replicator.getConfig().getId();

		@Override
		public void run()
		{
			StringBuffer buffer = new StringBuffer("(");

			for(Queue<CRDTCompiledTransaction> txnQueue : queues.values())
			{
				buffer.append(txnQueue.size());
				buffer.append(",");
			}

			if(buffer.charAt(buffer.length() - 1) == ',')
				buffer.setLength(buffer.length() - 1);

			buffer.append(")");

			LOG.info("<r{}> pending queue size: {}", id, buffer.toString());
		}
	}


	private class DeliveryThread implements Runnable
	{

		@Override
		public void run()
		{
			boolean hasDelivered;

			do
			{
				hasDelivered = false;

				for(Queue<CRDTCompiledTransaction> txnQueue : queues.values())
				{
					CRDTCompiledTransaction txn = txnQueue.poll();

					if(txn == null)
						continue;

					try
					{
						replicator.commitOperation(txn, true);
						hasDelivered = true;
					} catch(TransactionCommitFailureException e)
					{
						LOG.error(e.getMessage());
					}
				}
			} while(hasDelivered);

			LOG.trace("no more remote operations to apply. Going to sleep");
		}
	}
}
