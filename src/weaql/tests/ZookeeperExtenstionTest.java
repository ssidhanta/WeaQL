package weaql.tests;

import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.Topology;
import org.apache.zookeeper.ZooKeeper;
import weaql.common.thrift.CoordinatorRequest;
import weaql.common.thrift.CoordinatorResponse;
import weaql.common.thrift.RequestValue;
import weaql.common.thrift.UniqueValue;
import weaql.server.agents.coordination.zookeeper.EZKCoordinationClient;


/**
 * Created by dnlopes on 09/07/15.
 */
public class ZookeeperExtenstionTest
{

	private static final int SESSION_TIMEOUT = 200000;


	public static void main(String[] args) throws Exception
	{
		if(args.length != 1)
		{
			System.err.println("Usage: <topologyFile>");
			System.exit(1);
		}

		String topologyFile = args[0];
		Topology.setupTopology(topologyFile);

		ZookeeperExtenstionTest tester = new ZookeeperExtenstionTest();
		tester.testExtension();
	}

	public void testExtension() throws Exception
	{
		ZooKeeper zooKeeper = new ZooKeeper(Topology.ZOOKEEPER_CONNECTION_STRING, SESSION_TIMEOUT, null);

		EZKCoordinationClient coordinationExtenstion = new EZKCoordinationClient(zooKeeper, 1);
		coordinationExtenstion.init(WeaQLEnvironment.EZK_EXTENSION_CODE);
		//coordinationExtenstion.cleanupDatabase();

		//zooKeeper.create("/coordination/uniques/w_id_warehouse_UNIQUE", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
		//		CreateMode.PERSISTENT);

		UniqueValue u1 = new UniqueValue("w_id_warehouse_UNIQUE", "valu2aasde12");
		UniqueValue u2 = new UniqueValue("w_id_warehouse_UNIQUE", "value211");
		UniqueValue u3 = new UniqueValue("w_id_warehouse_UNIQUE", "value3");
		UniqueValue u4 = new UniqueValue("w_id_warehouse_UNIQUE", "value4");
		RequestValue requestValue = new RequestValue();
		requestValue.setConstraintId("o_id_orders_AUTO_INCREMENT");
		requestValue.setTempSymbol("@symbol");
		requestValue.setOpId(1);
		requestValue.setFieldName("test");

		CoordinatorRequest request = new CoordinatorRequest();
		request.addToRequests(requestValue);
		request.addToUniqueValues(u1);
		//request.addToUniqueValues(u2);
		//request.addToUniqueValues(u4);
		//request.addToUniqueValues(u3);

		CoordinatorResponse response = coordinationExtenstion.sendRequest(request);

		coordinationExtenstion.closeExtension();
		System.out.println("finished");

	}
}
