package weaql.common.util;


import weaql.common.nodes.NodeConfig;
import weaql.common.util.defaults.DatabaseDefaults;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * Created by dnlopes on 28/10/15.
 */
public class ConnectionFactory
{

	public static final String DEFAULT_DRIVER = "com.mysql.jdbc.Driver";

	static
	{
		try
		{
			Class.forName(DEFAULT_DRIVER);
		} catch(ClassNotFoundException e)
		{
			RuntimeUtils.throwRunTimeException(e.getMessage(), ExitCode.CLASS_NOT_FOUND);
		}
	}

	public static Connection getDefaultConnection(NodeConfig nodeInfo) throws SQLException
	{
		StringBuffer url = new StringBuffer(DatabaseDefaults.DEFAULT_URL_PREFIX);
		url.append(nodeInfo.getDbProps().getDbHost());
		url.append(":");
		url.append(nodeInfo.getDbProps().getDbPort());
		url.append("/");
		url.append(WeaQLEnvironment.DATABASE_NAME);
                Connection c = DriverManager.getConnection(url.toString(), nodeInfo.getDbProps().getDbUser(),
				nodeInfo.getDbProps().getDbPwd());
		c.setAutoCommit(false);
                //System.out.println("**** getdefaultconnection url tostring:="+url.toString());
		return c;
	}

	public static Connection getDefaultConnection(DatabaseProperties props, String databaseName) throws SQLException
	{
		StringBuffer buffer = new StringBuffer(DatabaseDefaults.DEFAULT_URL_PREFIX);
		buffer.append(props.getDbHost());
		buffer.append(":");
		buffer.append(props.getDbPort());
		buffer.append("/");
		buffer.append(databaseName);

		Connection c = DriverManager.getConnection(buffer.toString(), props.getDbUser(), props.getDbPwd());
		c.setAutoCommit(false);

		return c;
	}

	public static Connection getDefaultConnection(String prefix, DatabaseProperties props, String databaseName)
			throws SQLException
	{
		StringBuffer buffer = new StringBuffer(prefix);
		buffer.append(props.getDbHost());
		buffer.append(":");
		buffer.append(props.getDbPort());
		buffer.append("/");
		buffer.append(databaseName);

		Connection c = DriverManager.getConnection(buffer.toString(), props.getDbUser(), props.getDbPwd());
		c.setAutoCommit(false);

		return c;
	}
}
