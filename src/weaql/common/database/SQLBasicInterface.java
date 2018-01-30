package weaql.common.database;


import weaql.common.nodes.NodeConfig;
import weaql.common.util.ConnectionFactory;

import java.sql.*;


/**
 * Created by dnlopes on 25/09/15.
 */
public class SQLBasicInterface implements SQLInterface
{

	private Connection dbConnection;
	private Statement statQ;
	private Statement statU;
	private Statement statBU;
	private boolean emptyBatch;

	public SQLBasicInterface(NodeConfig config) throws SQLException
	{
		this.dbConnection = ConnectionFactory.getDefaultConnection(config);
	}

	public SQLBasicInterface(Connection con) throws SQLException
	{
		this.dbConnection = con;
		this.emptyBatch = true;

		this.statQ = this.dbConnection.createStatement();
		this.statU = this.dbConnection.createStatement();
		this.statBU = this.dbConnection.createStatement();
	}

	@Override
	public int executeUpdate(String sql) throws SQLException
	{
            return this.statU.executeUpdate(sql);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException
	{
            return this.statQ.executeQuery(sql);
	}

	@Override
	public int executeBatch() throws SQLException
	{
		if(!this.emptyBatch)
		{
			try
			{
				int[] res = this.statBU.executeBatch();
				int resultCount = 0;

				for(int i = 0; i < res.length; i++)
					resultCount += res[i];
                                return resultCount;

			} catch(SQLException e)
			{
				this.statBU.clearBatch();
				this.emptyBatch = true;
				throw e;
			}
		}

		return 0;
	}

	@Override
	public void addToBatchUpdate(String sql) throws SQLException
	{
		this.statBU.addBatch(sql);
		this.emptyBatch = false;
	}

	@Override
	public Connection getConnection()
	{
		return this.dbConnection;
	}

	@Override
	public void commit() throws SQLException
	{
            this.dbConnection.commit();
	}

	@Override
	public void rollback() throws SQLException
	{
		this.dbConnection.rollback();
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		return this.dbConnection.prepareStatement(sql);
	}
}
