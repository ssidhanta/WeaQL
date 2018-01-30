package weaql.tests;


import weaql.client.jdbc.CRDTConnectionFactory;
import weaql.common.nodes.NodeConfig;
import weaql.common.util.DatabaseProperties;
import weaql.common.util.Topology;
import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.exception.ConfigurationLoadException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by dnlopes on 22/12/15.
 */
public class DeltaMergeTest
{

	public static void main(String args[]) throws SQLException, ConfigurationLoadException, ClassNotFoundException
	{

		String topologyFile = "/Users/dnlopes/projects/thesis/code/weakdb/resources/topologies/localhost_micro_1node"
				+ ".xml";

		Topology.setupTopology(topologyFile);
		String envFile = "/Users/dnlopes/projects/thesis/code/weakdb/resources/environment/env_localhost_micro_default" +
				".env";

		System.setProperty("proxyid", String.valueOf(1));


		WeaQLEnvironment.setupEnvironment(envFile);

		NodeConfig config = Topology.getInstance().getProxyConfigWithIndex(1);

		DatabaseProperties props = config.getDbProps();

		Connection con = CRDTConnectionFactory.getCRDTConnection(props, "micro_crdt");
		Statement stat = con.createStatement();
		String op = "UPDATE t1 set c=c+10 where a=2";
		stat.executeUpdate(op);
		String op2 = "UPDATE t1 set c=c+5 where a=2";
		stat.executeUpdate(op2);
		String op3 = "UPDATE t1 set c=c-2 where a=2";
		stat.executeUpdate(op3);

		String op4 = "INSERT INTO t1 (a,b,c,d,e) values (12000,5555,6000,7777,'coco')";
		stat.executeUpdate(op4);
		String op5 = "UPDATE t1 set c=c-2 where a=12000";
		stat.executeUpdate(op5);
		con.commit();
	}
}
