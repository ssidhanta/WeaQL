package applications;


import java.sql.Connection;
//import weaql.client.proxy.SandboxExecutionProxy;


/**
 * Created by dnlopes on 05/09/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public interface Transaction
{

	boolean executeTransaction(Connection con);//,  SandboxExecutionProxy proxy);
	boolean commitTransaction(Connection con);//,  SandboxExecutionProxy proxy);
	String getLastError();
	boolean isReadOnly();
	String getName();
}
