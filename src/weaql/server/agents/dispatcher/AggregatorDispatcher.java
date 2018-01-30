package weaql.server.agents.dispatcher;


import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.server.replicator.IReplicatorNetwork;
import weaql.server.replicator.Replicator;

import java.util.*;
import java.util.concurrent.*;


/**
 * Created by dnlopes on 08/10/15.
 * AggregatorDispatcher caches incoming transactions and merges some of them when possible (for instance 2
 * consecutives updates to the same @LWW field). Periodically, transactions are sent as a batch to remote replicators
 */
public class AggregatorDispatcher implements DispatcherAgent
{

	private static final int THREAD_WAKEUP_INTERVAL = 500;

	private final IReplicatorNetwork networkInterface;
	private final ScheduledExecutorService scheduleService;
	private Queue<CRDTCompiledTransaction> pendingTransactions;

	public AggregatorDispatcher(Replicator replicator)
	{
		this.networkInterface = replicator.getNetworkInterface();
		this.pendingTransactions = new ConcurrentLinkedQueue<>();

		DispatcherThread deliveryThread = new DispatcherThread();

		this.scheduleService = Executors.newScheduledThreadPool(1);
		this.scheduleService.scheduleAtFixedRate(deliveryThread, 0, THREAD_WAKEUP_INTERVAL, TimeUnit.MILLISECONDS);
	}

	@Override
	public void dispatchTransaction(CRDTCompiledTransaction op)
	{
		pendingTransactions.add(op);
	}

	private class DispatcherThread implements Runnable
	{

		@Override
		public void run()
		{
			//TODO implement
			// 1) iterate over pending
			// 2) merge trx
			// 3) send batch
			// 4) send
			Queue<CRDTCompiledTransaction> snapshot = pendingTransactions;
			pendingTransactions = new PriorityBlockingQueue<>();

			List<CRDTCompiledTransaction> batch = prepareBatch(snapshot);

			networkInterface.send(batch);
		}

		private List<CRDTCompiledTransaction> prepareBatch(Queue<CRDTCompiledTransaction> trxList)
		{
			return new ArrayList<>(trxList);
		}
	}

}
