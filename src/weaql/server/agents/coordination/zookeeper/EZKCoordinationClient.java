package weaql.server.agents.coordination.zookeeper;


import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.extension.EZKExtensionRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.thrift.CoordinatorRequest;
import weaql.common.thrift.CoordinatorResponse;
import weaql.common.thrift.ThriftUtils;

import java.io.File;


/**
 * Created by dnlopes on 13/07/15.
 */
public class EZKCoordinationClient implements EZKCoordinationService
{

	private static final Logger LOG = LoggerFactory.getLogger(EZKCoordinationClient.class);

	private final String privateNode;
	private final int id;
	private final ZooKeeper zooKeeper;

	public EZKCoordinationClient(ZooKeeper zooKeeper, int id)
	{
		this.id = id;
		this.privateNode = EZKCoordinationExtension.ZOOKEEPER_BASE_NODE + File.separatorChar + "tmp"
				+ File.separatorChar +
				this.id;
		this.zooKeeper = zooKeeper;
	}

	@Override
	public void init(String codeBasePath) throws KeeperException, InterruptedException
	{
		EZKExtensionRegistration.registerExtension(this.zooKeeper, EZKCoordinationExtension.class, codeBasePath);

		// create private tmp node to hold generic byte array for responses
		Stat stat = this.zooKeeper.exists(this.privateNode, false);
		if(stat == null)
			this.zooKeeper.create(this.privateNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		LOG.debug("Coordination extension successfully installed at Zookeeper");
	}

	@Override
	public CoordinatorResponse sendRequest(CoordinatorRequest request)
	{
		boolean singleRpc = !request.isSetRequests() || request.getRequestsSize() == 0;

		request.setTempNodePath(this.privateNode);

		if(singleRpc)
			return this.singleRpcCoordination(request);
		else
			return this.twoRpcCoordination(request);
	}

	@Override
	public void cleanupDatabase() throws KeeperException, InterruptedException
	{
		this.zooKeeper.setData(EZKCoordinationExtension.CLEANUP_OP_CODE, new byte[0], -1);
	}

	@Override
	public void closeExtension() throws InterruptedException
	{
		this.zooKeeper.close();
	}

	private CoordinatorResponse singleRpcCoordination(CoordinatorRequest request)
	{
		byte[] bytesRequest = ThriftUtils.encodeThriftObject(request);
		CoordinatorResponse response = new CoordinatorResponse();
		response.setSuccess(false);

		try
		{
			// sync call to reserve unique values
			Stat stat = this.zooKeeper.setData(EZKCoordinationExtension.REQUEST_OP_CODE, bytesRequest, -1);

			if(stat.getVersion() == 0)
				response.setSuccess(true);

		} catch(KeeperException | InterruptedException e)
		{
			if(LOG.isWarnEnabled())
				LOG.warn(e.getMessage());

			response.setSuccess(false);
		}

		return response;
	}

	private CoordinatorResponse twoRpcCoordination(CoordinatorRequest request)
	{
		byte[] bytesRequest = ThriftUtils.encodeThriftObject(request);
		CoordinatorResponse response = new CoordinatorResponse();
		response.setSuccess(false);

		// async call to reserve unique values
		this.zooKeeper.setData(EZKCoordinationExtension.REQUEST_OP_CODE, bytesRequest, -1, null, null);

		// then, read privateNode data to get the requested values
		this.readPrivateNode(response);

		return response;
	}

	private void readPrivateNode(CoordinatorResponse response)
	{
		try
		{
			byte[] responseByteArray = this.zooKeeper.getData(this.privateNode, false, null);
			CoordinatorResponse tmpResponse = decodeCoordinatorResponse(responseByteArray);
			response.setSuccess(tmpResponse.isSuccess());
			response.setRequestedValues(tmpResponse.getRequestedValues());

		} catch(KeeperException | InterruptedException e)
		{
			response.setSuccess(false);
		}
	}

	private CoordinatorResponse decodeCoordinatorResponse(byte[] bytesObject)
	{
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
		CoordinatorResponse req = new CoordinatorResponse();
		try
		{
			deserializer.deserialize(req, bytesObject);
			return req;
		} catch(TException e)
		{
			return null;
		}
	}

}
