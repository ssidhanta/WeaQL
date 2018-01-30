package weaql.tests;


import weaql.client.jdbc.CRDTConnectionFactory;
import weaql.common.util.DatabaseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by dnlopes on 19/06/15.
 */
public class SQLTester
{
	private final static String DB_HOST = "172.16.24.228";

	public static void main(String[] args) throws SQLException, ClassNotFoundException
	{
		DatabaseProperties props = new DatabaseProperties("sa", "101010", DB_HOST, 3306);

		System.setProperty("proxyid", "1");
		System.setProperty("usersNum", "1");
		System.setProperty("configPath",
				"/Users/dnlopes/devel/thesis/code/weakdb/resources/configs/micro_localhost_1node.xml");


		Connection conn = CRDTConnectionFactory.getCRDTConnection(props, "micro");
		Statement stat = conn.createStatement();


		// delete parent
		//stat.executeUpdate("DELETE FROM t1 where a=14");

		// delete child/neutral
		//stat.executeUpdate("DELETE FROM t2 where a=75");

		// insert child
		//stat.executeUpdate("INSERT INTO t2 (a,b,c,d,e) VALUES (9999,100,10,10,'CENAS')");

		//insert neutral/parent
		stat.executeUpdate("INSERT INTO t1 (a,b,c,d,e) VALUES (2323123,98880,10,10,'CENAS')");
		stat.executeUpdate("UPDATE t1 set c=c-9 where a=2323123");

		// update child
		//stat.executeUpdate("UPDATE t2 set e='COCO' where a=300");

		// update neutral
		//stat.executeUpdate("UPDATE t1 set e='TESTE1' where a=100");

		// update parent
		//stat.executeUpdate("UPDATE t1 set b=221 where a=22");

		conn.commit();
	}
}
