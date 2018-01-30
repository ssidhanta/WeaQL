package weaql.client.jdbc;

import java.sql.*;
import java.sql.Connection;
import java.sql.Driver;
import java.util.Properties;
import java.util.logging.Logger;


/**
 * Created by dnlopes on 10/02/15.
 */
public class CRDTDriver implements Driver
{
	public static final String CRDT_URL_PREFIX = "jdbc:crdt://";

	static
	{
		try
		{
			DriverManager.registerDriver(new CRDTDriver());

		} catch(SQLException e)
		{
			throw new RuntimeException("Error: failed to register crdt:Driver");
		}
	}

	private CRDTDriver()
	{
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException
	{
		if(this.acceptsURL(url))
			return new CRDTConnection();

		return null;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException
	{
		return url.startsWith(CRDT_URL_PREFIX);
	}


	/* Stubs */

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
	{
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion()
	{
		return 0;
	}

	@Override
	public int getMinorVersion()
	{
		return 0;
	}

	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		return null;
	}
}
