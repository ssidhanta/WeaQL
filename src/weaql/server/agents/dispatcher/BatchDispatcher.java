package weaql.server.agents.dispatcher;


import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.server.replicator.IReplicatorNetwork;
import weaql.server.replicator.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;


/**
 * Created by dnlopes on 21/10/15.
 * A SimpleBatchDisptacher simply groups transactions in a batch and, periodically, sends the batch to remote
 * replicators
 */
public class BatchDispatcher implements DispatcherAgent
{
	
	private static final Logger LOG = LoggerFactory.getLogger(BatchDispatcher.class);
	private static final int THREAD_WAKEUP_INTERVAL = 2000;

	private final IReplicatorNetwork networkInterface;
	private final ScheduledExecutorService scheduleService;
	private Queue<CRDTCompiledTransaction> pendingTransactions;

	public BatchDispatcher(Replicator replicator)
	{
		this.networkInterface = replicator.getNetworkInterface();
		this.pendingTransactions = new ConcurrentLinkedQueue<>();

		BatchSenderThread batchSender = new BatchSenderThread();

		this.scheduleService = Executors.newScheduledThreadPool(1);
		this.scheduleService.scheduleAtFixedRate(batchSender, 0, THREAD_WAKEUP_INTERVAL, TimeUnit.MILLISECONDS);
	}

	@Override
	public void dispatchTransaction(CRDTCompiledTransaction op)
	{
		pendingTransactions.add(op);
	}

	private class BatchSenderThread implements Runnable
	{

		@Override
		public void run()
		{
			//TODO fix bug: check concurrency when changing pointers
			Queue<CRDTCompiledTransaction> snapshot = pendingTransactions;
			pendingTransactions = new ConcurrentLinkedQueue<>();

			List<CRDTCompiledTransaction> batch = prepareBatch(snapshot);

			LOG.debug("sending txn batch (size {}) to remote nodes", batch.size());
			networkInterface.send(batch);
		}

		private List<CRDTCompiledTransaction> prepareBatch(Queue<CRDTCompiledTransaction> trxList)
		{
			return new ArrayList<>(trxList);
		}
	}
}