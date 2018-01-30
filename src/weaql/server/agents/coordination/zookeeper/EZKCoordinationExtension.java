package weaql.server.agents.coordination.zookeeper;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.extension.EZKBaseExtension;
import org.apache.zookeeper.server.EZKExtensionGate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.thrift.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * Created by dnlopes on 13/07/15.
 */
public class EZKCoordinationExtension extends EZKBaseExtension
{

	private static final Logger LOG = LoggerFactory.getLogger(EZKCoordinationExtension.class);

	public static final String ZOOKEEPER_BASE_NODE = "/coordination";
	public static final String OP_PREFIX = ZOOKEEPER_BASE_NODE;
	public static final String CLEANUP_OP_CODE = OP_PREFIX + File.separatorChar + "cleanup";
	public static final String REQUEST_OP_CODE = OP_PREFIX + File.separatorChar + "request";
	public static final String TMP_DIR = ZOOKEEPER_BASE_NODE + File.separatorChar + "tmp";
	public static final String UNIQUE_DIR = ZOOKEEPER_BASE_NODE + File.separatorChar + "uniques";
	public static final String COUNTERS_DIR = ZOOKEEPER_BASE_NODE + File.separatorChar + "counters";

	public EZKCoordinationExtension()
	{
	}

	@Override
	public boolean matchesOperation(int requestType, String path)
	{
		return path.startsWith(OP_PREFIX);
	}

	@Override
	public void init() throws KeeperException
	{
		Stat stat = extensionGate.exists(ZOOKEEPER_BASE_NODE, false);
		if(stat == null)
			this.extensionGate.create(ZOOKEEPER_BASE_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);

		stat = extensionGate.exists(TMP_DIR, false);
		if(stat == null)
			this.extensionGate.create(TMP_DIR, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		stat = extensionGate.exists(UNIQUE_DIR, false);
		if(stat == null)
			this.extensionGate.create(UNIQUE_DIR, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		stat = extensionGate.exists(COUNTERS_DIR, false);
		if(stat == null)
			this.extensionGate.create(COUNTERS_DIR, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		if(LOG.isInfoEnabled())
			LOG.info("EZKCoordination extension initialized");
	}

	@Override
	protected Stat setData(String path, byte[] data, int version) throws KeeperException
	{
		if(path.compareTo(CLEANUP_OP_CODE) == 0)
		{
			this.handleCleanupOperation();
			return EZKExtensionGate.SUCCESS_STAT;

		} else if(path.startsWith(REQUEST_OP_CODE))
			return this.handleRequestOperation(path, data);
		else
		{
			if(LOG.isWarnEnabled())
				LOG.warn("unexpected operation code. Executing default 'setData' implementation");

			return this.extensionGate.setData(path, data, version);
		}
	}

	private void handleCleanupOperation() throws KeeperException
	{
		try
		{
			this.cleanupDatabase();
		} catch(KeeperException e)
		{
			if(LOG.isErrorEnabled())
				LOG.error("failed to cleanup zookeeper database");
			throw e;
		}
	}

	private Stat handleRequestOperation(String path, byte[] data) throws KeeperException
	{
		CoordinatorRequest request = ThriftUtils.decodeCoordinatorRequest(data);

		boolean success = this.lockNodes(request.getUniqueValues());

		if(!success)
			return EZKExtensionGate.EXCEPTION_STAT;

		if(request.isSetRequests())
			this.prepareRequestedValues(request.getTempNodePath(), request);

		return EZKExtensionGate.SUCCESS_STAT;
	}

	private boolean lockNodes(List<UniqueValue> valuesList) throws KeeperException
	{
		if(valuesList == null || valuesList.size() == 0)
			return true;

		if(LOG.isInfoEnabled())
			LOG.info("reserving {} values", valuesList.size());

		boolean success = true;
		StringBuilder buffer = new StringBuilder();

		for(UniqueValue uniqueValue : valuesList)
		{
			buffer.setLength(0);
			buffer.append(EZKCoordinationExtension.UNIQUE_DIR + File.separatorChar);
			buffer.append(uniqueValue.getConstraintId());
			buffer.append(File.separatorChar);
			buffer.append(uniqueValue.getValue());
			String nodePath = buffer.toString();

			if(!this.tryLockNode(nodePath))
				throw new KeeperException.NodeExistsException();
		}

		if(LOG.isInfoEnabled())
			LOG.info("done!");

		return success;
	}

	private void prepareRequestedValues(String nodePath, CoordinatorRequest request) throws KeeperException
	{
		List<RequestValue> requestValuesList = request.getRequests();

		if(requestValuesList == null || requestValuesList.size() <= 0)
		{
			if(LOG.isWarnEnabled())
				LOG.warn("RequestValue obj is set but no entries were found!");

			return;
		}

		CoordinatorResponse response = new CoordinatorResponse();
		response.setSuccess(true);

		for(RequestValue requestValue : requestValuesList)
		{
			try
			{
				int counter = askForValue(requestValue);
				requestValue.setRequestedValue(String.valueOf(counter));
				response.addToRequestedValues(requestValue);
			} catch(KeeperException e)
			{
				// if something bad happens, set response success to false and leave
				response.setSuccess(false);
				break;
			}
		}

		byte[] encodedResponse = ThriftUtils.encodeThriftObject(response);
		this.extensionGate.setData(nodePath, encodedResponse, -1);
	}

	private int askForValue(RequestValue reqValue) throws KeeperException
	{
		String pathNode = COUNTERS_DIR + File.separatorChar + reqValue.getConstraintId();
		return this.incrementAndGet(pathNode);
	}

	private boolean tryLockNode(String node)
	{
		// try to create node
		try
		{
			extensionGate.create(node, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return true;
		} catch(KeeperException e)
		{
			if(LOG.isWarnEnabled())
				LOG.warn(e.getMessage());
			return false;
		}
	}

	private void cleanupDatabase() throws KeeperException
	{
		if(LOG.isInfoEnabled())
		{
			LOG.info("cleaning up database nodes");
			LOG.info("deleting {} directory...", UNIQUE_DIR);
		}

		this.deleteDirectory(UNIQUE_DIR, false);

		if(LOG.isInfoEnabled())
			LOG.info("deleting {} directory...", COUNTERS_DIR);

		this.deleteDirectory(COUNTERS_DIR, false);

		if(LOG.isInfoEnabled())
			LOG.info("database cleaned");
	}

	private void deleteDirectory(String path, boolean deleteSelf) throws KeeperException
	{
		List<String> childrens;
		childrens = this.extensionGate.getChildren(path, false, null);

		if(childrens.size() > 0)
		{
			LOG.info("erasing {} nodes from {}", childrens.size(), path);

			for(String children : childrens)
				this.deleteDirectory(path + File.separatorChar + children, true);
		}

		if(deleteSelf)
			this.extensionGate.delete(path, -1);
	}

	private int incrementAndGet(String nodePath) throws KeeperException
	{
		boolean success;
		int newValue, oldValue;

		do
		{
			Stat nodeStat = new Stat();
			oldValue = fromBytes(this.extensionGate.getData(nodePath, false, nodeStat));
			newValue = oldValue + 1;
			success = this.tryIncrement(newValue, nodePath, nodeStat);

		} while(!success);

		return newValue;
	}

	public boolean tryIncrement(int newValue, String nodePath, Stat nodeStat)
	{
		try
		{
			this.extensionGate.setData(nodePath, toBytes(newValue), nodeStat.getVersion());
			return true;
		} catch(KeeperException ignore)
		{
			return false;
		}
	}

	private static byte[] toBytes(int value)
	{
		byte[] bytes = new byte[4];
		ByteBuffer.wrap(bytes).putInt(value);
		return bytes;
	}

	private static int fromBytes(byte[] bytes)
	{
		return ByteBuffer.wrap(bytes).getInt();
	}

	public interface ZookeeperDefaults
	{
		int ZOOKEEPER_SESSION_TIMEOUT = 200000;
		int ZOOKEEPER_DEFAULT_PORT = 2181;
	}

}
