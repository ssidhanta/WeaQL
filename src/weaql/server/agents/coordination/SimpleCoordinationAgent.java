package weaql.server.agents.coordination;


import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.defaults.ReplicatorDefaults;
import weaql.common.util.exception.InitComponentFailureException;
import weaql.common.util.exception.InvalidConfigurationException;
import weaql.common.util.exception.SocketConnectionException;
import weaql.server.replicator.IReplicatorNetwork;
import weaql.server.replicator.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.thrift.*;
import weaql.server.util.CoordinationFailureException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by dnlopes on 22/10/15.
 */
public class SimpleCoordinationAgent implements CoordinationAgent
{

	private static final Logger LOG = LoggerFactory.getLogger(SimpleCoordinationAgent.class);

	private Replicator replicator;
	private IReplicatorNetwork network;
	private ScheduledExecutorService scheduleService;
	private AtomicLong latencySum;
	private AtomicInteger eventsCounter;

	public SimpleCoordinationAgent(Replicator replicator) throws InitComponentFailureException
	{
		this.latencySum = new AtomicLong();
		this.eventsCounter = new AtomicInteger();

		if(WeaQLEnvironment.IS_ZOOKEEPER_REQUIRED)
		{
			this.replicator = replicator;
			this.network = this.replicator.getNetworkInterface();

			this.scheduleService = Executors.newScheduledThreadPool(1);
			this.scheduleService.scheduleAtFixedRate(new StateChecker(), 0,
					ReplicatorDefaults.STATE_CHECKER_THREAD_INTERVAL, TimeUnit.MILLISECONDS);
		} else
			LOG.info("zookeeper is not required in this environment");
	}

	@Override
	public void handleCoordination(CRDTPreCompiledTransaction trx)
			throws SocketConnectionException, CoordinationFailureException, InvalidConfigurationException
	{
		if(!trx.isSetRequestToCoordinator())
		{
			trx.setReadyToCommit(true);
			return;
		}

		if(!WeaQLEnvironment.IS_ZOOKEEPER_REQUIRED)
			throw new InvalidConfigurationException(
					"current environment does not require coordination but proxy " + "generated a request");

		eventsCounter.incrementAndGet();
		CoordinatorRequest request = trx.getRequestToCoordinator();
		long beginTime = System.nanoTime();
		CoordinatorResponse response = this.network.sendRequestToCoordinator(request);
		long estimatedTime = System.nanoTime() - beginTime;
		latencySum.addAndGet(estimatedTime);

		if(response.isSuccess())
		{
			handleCoordinatorResponse(response, trx);
			trx.setReadyToCommit(true);
		} else
		{
			trx.setReadyToCommit(false);
			throw new CoordinationFailureException(response.getErrorMessage());
		}
	}

	private void handleCoordinatorResponse(CoordinatorResponse response, CRDTPreCompiledTransaction transaction)
			throws CoordinationFailureException
	{
		if(response.isSetRequestedValues())
		{
			List<RequestValue> requestedValues = response.getRequestedValues();
			replaceSymbolsForValues(requestedValues, transaction);
		}
	}

	private void replaceSymbolsForValues(List<RequestValue> requestedValues, CRDTPreCompiledTransaction transaction)
			throws CoordinationFailureException
	{
		Map<String, SymbolEntry> symbols = transaction.getSymbolsMap();

		// lets replace the symbols for the values received from coordinator
		for(RequestValue requestValue : requestedValues)
		{
			if(requestValue.isSetRequestedValue())
			{
				SymbolEntry symbolEntry = symbols.get(requestValue.getTempSymbol());
				if(requestValue.getRequestedValue() == null)
					throw new CoordinationFailureException("failed to retrieve id from coordinator");

				symbolEntry.setRealValue(requestValue.getRequestedValue());
			}
		}
	}

	private class StateChecker implements Runnable
	{

		private int id = replicator.getConfig().getId();

		@Override
		public void run()
		{
			int coordinatedRequests = eventsCounter.get();
			if(coordinatedRequests == 0)
				return;

			double avgLatency = (latencySum.get() / coordinatedRequests) * 0.000001;

			//LOG.info("<r{}> {} coordinated events with average latency of {}", id, coordinatedRequests, avgLatency);
		}
	}
}
