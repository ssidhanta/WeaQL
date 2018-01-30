package weaql.server.agents;


import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.exception.InvalidConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.agents.deliver.CausalDeliverAgent;
import weaql.server.agents.deliver.DeliverAgent;
import weaql.server.agents.deliver.NoOrderDeliverAgent;
import weaql.server.agents.dispatcher.BasicDispatcher;
import weaql.server.agents.dispatcher.BatchDispatcher;
import weaql.server.agents.dispatcher.DispatcherAgent;
import weaql.server.replicator.Replicator;


/**
 * Created by dnlopes on 28/10/15.
 */
public class AgentsFactory
{

	private static final Logger LOG = LoggerFactory.getLogger(AgentsFactory.class);

	public static DeliverAgent createDeliverAgent(Replicator replicator) throws InvalidConfigurationException
	{
		switch(WeaQLEnvironment.DELIVER_AGENT)
		{
		case 1:
			return new CausalDeliverAgent(replicator);
		case 2:
			return new NoOrderDeliverAgent(replicator);
		default:
			throw new InvalidConfigurationException("unknown deliver agent class");
		}
	}

	public static DispatcherAgent createDispatcherAgent(Replicator replicator) throws InvalidConfigurationException
	{
		// 1=BatchDispatcher, 2=BasicDispatcher, 3=AggregatorDispatcher
		switch(WeaQLEnvironment.DISPATCHER_AGENT)
		{
		case 1:
			return new BatchDispatcher(replicator);
		case 2:
			return new BasicDispatcher(replicator);
		case 3:
			throw new InvalidConfigurationException("specified dispatcher agent not yet implemented");
		default:
			throw new InvalidConfigurationException("unknown dispatcher agent class");
		}
	}

	public static String getDeliverAgentClassAsString()
	{
		switch(WeaQLEnvironment.DELIVER_AGENT)
		{
		case 1:
			return "CausalDeliverAgent";
		case 2:
			return "NoOrderDeliverAgent";
		default:
			LOG.error("unkown deliver agent class");
			return null;
		}
	}

	public static String getDispatcherAgentClassAsString()
	{
		switch(WeaQLEnvironment.DISPATCHER_AGENT)
		{
		case 1:
			return "BatchDispatcher";
		case 2:
			return "BasicDispatcher";
		case 3:
			return "AggregatorDispatcher";
		default:
			LOG.error("unkown dispatcher agent class");
			return null;
		}
	}
}
