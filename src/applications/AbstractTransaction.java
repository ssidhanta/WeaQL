package applications;


import java.sql.Connection;
import java.sql.SQLException;
//import weaql.client.proxy.SandboxExecutionProxy;


/**
 * Created by dnlopes on 02/12/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public abstract class AbstractTransaction implements Transaction
{

	protected String lastError;
	protected String txnName;

	public AbstractTransaction(String name)
	{
		this.txnName = name;
	}

	@Override
	public String getLastError()
	{
		return this.lastError;
	}
        
        @Override
	public boolean commitTransaction(Connection con)//,  SandboxExecutionProxy proxy)
	{
		try
		{
			//proxy.commit();
                        con.commit();
			return true;

		} catch(SQLException e)
		{
			lastError = e.getMessage()+ " in stacktrace: \n" + e.getStackTrace().toString();
			this.rollbackQuietly(con);
			return false;
		}
	}

	protected void rollbackQuietly(Connection connection)
	{
		try
		{
			connection.rollback();
		} catch(SQLException ignored)
		{

		}
	}

	public String getName()
	{
		return this.txnName;
	}
}
