package weaql.client.proxy;


import weaql.client.proxy.log.TransactionLog;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * Created by dnlopes on 02/09/15.
 */
public interface Proxy
{
	int getProxyId();
	void setReadOnly(boolean readOnly);

	void abort();
	void commit() throws SQLException;
	void close() throws SQLException;

	ResultSet executeQuery(String op) throws SQLException;
	int executeUpdate(String op) throws SQLException;
	TransactionLog getTransactionLog();
}
