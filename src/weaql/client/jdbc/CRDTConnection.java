package weaql.client.jdbc;


import weaql.client.proxy.Proxy;
import weaql.client.proxy.ProxyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.ExitCode;
import weaql.common.util.exception.MissingImplementationException;

import java.sql.*;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


/**
 * Created by dnlopes on 10/02/15.
 */
public class CRDTConnection implements Connection
{

	private static final Logger LOG = LoggerFactory.getLogger(CRDTConnection.class);

	private final int connectionId;
	private final Proxy proxy;

	public CRDTConnection() throws SQLException
	{
		this.proxy = ProxyFactory.getProxyInstance();
		this.connectionId = this.proxy.getProxyId();
	}

	@Override
	public Statement createStatement() throws SQLException
	{
		return new CRDTStatement(proxy);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		return new CRDTPreparedStatement(sql, proxy);
	}

	@Override
	public void commit() throws SQLException
	{   
                //System.out.println("****In CRDT Connection commit()");
		this.proxy.commit();
	}

	@Override
	public void rollback() throws SQLException
	{
		if(LOG.isTraceEnabled())
			LOG.trace("aborting txn on connection {} by user request", this.connectionId);

		this.proxy.abort();
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException
	{
		if(autoCommit)
			throw new SQLException("connection auto commit not supported");
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		this.proxy.setReadOnly(readOnly);
	}

	@Override
	public void close() throws SQLException
	{
		proxy.close();
	}


/*
	NOT IMPLEMENTED METHODS START HERE
*/

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public String nativeSQL(String sql) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		return false;
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return false;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public String getCatalog() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException
	{

		//throw new MissingImplementationException("missing implementation");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException
	{
		return new CRDTPreparedStatement(sql, proxy);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void setHoldability(int holdability) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public int getHoldability() throws SQLException
	{
		return 0;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
											  int resultSetHoldability) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
										 int resultSetHoldability) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
	{
		return new CRDTPreparedStatement(sql, proxy);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Clob createClob() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public boolean isValid(int timeout) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException
	{
		System.exit(ExitCode.MISSING_IMPLEMENTATION);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException
	{
		System.exit(ExitCode.MISSING_IMPLEMENTATION);
	}

	@Override
	public String getClientInfo(String name) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void setSchema(String schema) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public String getSchema() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void abort(Executor executor) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		throw new MissingImplementationException("missing implementation");
	}
}
