package weaql.server.replicator;


import weaql.common.nodes.AbstractNetwork;
import weaql.common.nodes.NodeConfig;
import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.Topology;
import weaql.common.util.exception.SocketConnectionException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weaql.common.util.ObjectPool;
import weaql.common.thrift.*;
import weaql.server.agents.coordination.zookeeper.EZKCoordinationClient;
import weaql.server.agents.coordination.zookeeper.EZKCoordinationExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class ReplicatorNetwork extends AbstractNetwork implements IReplicatorNetwork
{

	private static final Logger LOG = LoggerFactory.getLogger(ReplicatorNetwork.class);

	private Map<Integer, NodeConfig> replicatorsConfigs;
	private ObjectPool<EZKCoordinationClient> ezkClientsPool;
	private Map<Integer, ObjectPool<ReplicatorRPC.Client>> replicatorsConnections;
	private AtomicInteger clientsCount;
	private final int deltaIncrement;

	public ReplicatorNetwork(NodeConfig node)
	{
		super(node);

		this.replicatorsConfigs = new HashMap<>();
		this.replicatorsConnections = new HashMap<>();
		this.clientsCount = new AtomicInteger(me.getId());
		this.ezkClientsPool = new ObjectPool<>();
		this.deltaIncrement = Topology.getInstance().getReplicatorsCount();

		for(NodeConfig replicatorConfig : Topology.getInstance().getAllReplicatorsConfig().values())
			if(replicatorConfig.getId() != me.getId())
			{
				replicatorsConnections.put(replicatorConfig.getId(), new ObjectPool<ReplicatorRPC.Client>());
				replicatorsConfigs.put(replicatorConfig.getId(), replicatorConfig);
			}
	}

	@Override
	public void send(CRDTCompiledTransaction transaction)
	{
		for(NodeConfig config : replicatorsConfigs.values())
		{
			ObjectPool<ReplicatorRPC.Client> pool = replicatorsConnections.get(config.getId());
			ReplicatorRPC.Client connection = pool.borrowObject();

			if(connection == null)
				connection = openConnection(config);

			if(connection == null)
			{
				LOG.error("could not contact peer (replicator {})", config.getId());
				continue;
			}

			try
			{
				connection.sendToRemote(transaction);
			} catch(TException e)
			{
				LOG.warn(e.getMessage());
			} finally
			{
				if(connection != null)
					pool.returnObject(connection);
			}
		}
	}

	@Override
	public void send(List<CRDTCompiledTransaction> transactions)
	{
		for(NodeConfig config : this.replicatorsConfigs.values())
		{
			TTransport newTransport = new TSocket(config.getHost(), config.getPort());

			try
			{
				newTransport.open();
				TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(newTransport);
				ReplicatorRPC.Client client = new ReplicatorRPC.Client(protocol);
				client.sendBatchToRemote(transactions);
			} catch(TException e)
			{
				LOG.warn("failed to send batch to replicator {}", config.getId(), e);
			} finally
			{
				newTransport.close();
			}
		}
	}

	@Override
	public void openConnections()
	{
		LOG.info("opening connections for peers");
		openZookeeperConnections();
		openReplicatorConnections();
	}

	@Override
	public CoordinatorResponse sendRequestToCoordinator(CoordinatorRequest req) throws SocketConnectionException
	{
		CoordinatorResponse dummyResponse = new CoordinatorResponse();
		dummyResponse.setSuccess(false);

		EZKCoordinationClient client = this.ezkClientsPool.borrowObject();

		if(client == null)
		{
			LOG.warn("zookeeper connections pool empty. creating new connection");

			client = createZookeeperConnection();

			if(client == null)
			{
				LOG.error("failed to create new zookeeper connection");
				throw new SocketConnectionException("failed to establish connection with zookeeper cluster");
			}
		}

		CoordinatorResponse response = client.sendRequest(req);
		this.ezkClientsPool.returnObject(client);

		return response;
	}

	private void openZookeeperConnections()
	{
		if(WeaQLEnvironment.IS_ZOOKEEPER_REQUIRED)
		{
			LOG.info("opening {} zookeeper connections", WeaQLEnvironment.EZK_CLIENTS_POOL_SIZE);

			for(int i = 0; i < WeaQLEnvironment.EZK_CLIENTS_POOL_SIZE; i++)
			{
				EZKCoordinationClient newClient = createZookeeperConnection();

				if(newClient != null)
					ezkClientsPool.addObject(newClient);
			}
			LOG.info("{} connections for zookeeper available", ezkClientsPool.getPoolSize());
		}
	}

	private void openReplicatorConnections()
	{
		for(NodeConfig config : this.replicatorsConfigs.values())
			openConnectionsForPeer(config);
	}

	private void openConnectionsForPeer(NodeConfig config)
	{
		int maxConnections = WeaQLEnvironment.REPLICATORS_CONNECTIONS_POOL_SIZE;
		ObjectPool<ReplicatorRPC.Client> pool = replicatorsConnections.get(config.getId());

		for(int i = 0; i < maxConnections; i++)
		{
			ReplicatorRPC.Client aConnection = openConnection(config);
			if(aConnection != null)
				pool.addObject(aConnection);
		}

		LOG.info("{} connections for replicator {} available", pool.getPoolSize(), config.getId());
	}

	private ReplicatorRPC.Client openConnection(NodeConfig config)
	{
		TTransport newTransport = new TSocket(config.getHost(), config.getPort());

		try
		{
			newTransport.open();
			TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(newTransport);
			return new ReplicatorRPC.Client(protocol);
		} catch(TException e)
		{
			newTransport.close();
			LOG.warn("***in open connection config.getHost(): "+ config.getHost() +" e.message: "+e.getMessage());
                        e.printStackTrace();
			return null;
		}
	}

	private EZKCoordinationClient createZookeeperConnection()
	{
		ZooKeeper zooKeeper;
		try
		{
			zooKeeper = new ZooKeeper(Topology.ZOOKEEPER_CONNECTION_STRING,
					EZKCoordinationExtension.ZookeeperDefaults.ZOOKEEPER_SESSION_TIMEOUT, null);
		} catch(IOException e)
		{
			LOG.warn(e.getMessage());
			return null;
		}

		int nextId = clientsCount.addAndGet(deltaIncrement);
		EZKCoordinationClient client = new EZKCoordinationClient(zooKeeper, nextId);

		try
		{
			client.init(WeaQLEnvironment.EZK_EXTENSION_CODE);
		} catch(KeeperException | InterruptedException e)
		{
			LOG.warn("failed to install zookeeper extension {}: ", e.getMessage(), e);
			return null;
		}

		return client;
	}
}
