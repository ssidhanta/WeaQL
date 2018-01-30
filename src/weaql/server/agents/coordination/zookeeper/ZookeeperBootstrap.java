package weaql.server.agents.coordination.zookeeper;


import weaql.common.database.constraints.Constraint;
import weaql.common.database.constraints.ConstraintType;
import weaql.common.database.constraints.unique.AutoIncrementConstraint;
import weaql.common.database.constraints.unique.UniqueConstraint;
import weaql.common.database.util.DatabaseMetadata;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.util.*;
import weaql.common.util.defaults.DatabaseDefaults;
import org.apache.commons.dbutils.DbUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.thrift.CoordinatorRequest;
import weaql.common.thrift.CoordinatorResponse;
import weaql.common.thrift.UniqueValue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


/**
 * Created by dnlopes on 14/07/15.
 */
public class ZookeeperBootstrap
{

	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperBootstrap.class);

	private static final int SESSION_TIMEOUT = 2000000;
	private final Connection connection;
	private final ZooKeeper zookeeper;
	private final EZKCoordinationService ezkClient;
	private final DatabaseMetadata databaseMetadata;

	public static void main(String args[]) throws Exception
	{
		if(args.length != 4)
		{
			LOG.error("usage: java -jar <jarfile> <environmentFile> <zookeeperHost> <databaseHost> <databaseName>");
			System.exit(ExitCode.WRONG_ARGUMENTS_NUMBER);
		}

		String environmentFile = args[0];
		String zookeeperHost = args[1];
		String databaseHost = args[2];
		String dbName= args[3];

		WeaQLEnvironment.setupEnvironment(environmentFile);

		ZookeeperBootstrap bootstrap = new ZookeeperBootstrap(zookeeperHost, databaseHost, dbName);
		bootstrap.installExtension();
		bootstrap.readDatabaseState();
		bootstrap.exitGracefully();
	}

	public ZookeeperBootstrap(String zookeeperHost, String databaseHost, String dbName) throws IOException, SQLException
	{
		this.databaseMetadata = WeaQLEnvironment.DB_METADATA;
		this.zookeeper = new ZooKeeper(zookeeperHost, SESSION_TIMEOUT, null);
		this.ezkClient = new EZKCoordinationClient(this.zookeeper, 1);
		DatabaseProperties props = new DatabaseProperties(DatabaseDefaults.DEFAULT_USER, DatabaseDefaults
				.DEFAULT_PASSWORD, databaseHost, DatabaseDefaults.DEFAULT_MYSQL_PORT);

		this.connection = ConnectionFactory.getDefaultConnection(props, dbName);
	}

	public void readDatabaseState()
	{
		CoordinatorRequest request = new CoordinatorRequest();
		request.setUniqueValues(new ArrayList<UniqueValue>());

		for(DatabaseTable table : this.databaseMetadata.getAllTables())
		{
			Set<Constraint> tableConstraints = new HashSet<>();
			tableConstraints.addAll(table.getAutoIncrementConstraints());
			tableConstraints.addAll(table.getUniqueConstraints());
			tableConstraints.addAll(table.getCheckConstraints());

			for(Constraint constraint : tableConstraints)
			{
				if(constraint.getType() == ConstraintType.UNIQUE && constraint.requiresCoordination())
					this.treatUniqueConstraint(table, (UniqueConstraint) constraint, request);
				else if(constraint.getType() == ConstraintType.AUTO_INCREMENT && constraint.requiresCoordination())
					this.treatAutoIncrementConstraint((AutoIncrementConstraint) constraint);
				else if(constraint.getType() == ConstraintType.CHECK)
						LOG.warn("check constraints not yet supported");
			}
		}
	}

	public void installExtension() throws Exception
	{
		this.ezkClient.init(WeaQLEnvironment.EZK_EXTENSION_CODE);
		this.ezkClient.cleanupDatabase();
	}

	private void treatUniqueConstraint(DatabaseTable dbTable, UniqueConstraint uniqueConstraint, CoordinatorRequest req)
	{
		if(!uniqueConstraint.requiresCoordination())
			return;

		if(uniqueConstraint.isPrimaryKey() && !dbTable.getTablePolicy().allowInserts())
			return;

		String prefix = EZKCoordinationExtension.UNIQUE_DIR + File.separatorChar + uniqueConstraint
				.getConstraintIdentifier();

		this.createNode(prefix);

		if(LOG.isTraceEnabled())
			LOG.trace("scanning all used values");

		StringBuilder buffer = new StringBuilder();

		Iterator<DataField> it = uniqueConstraint.getFields().iterator();

		while(it.hasNext())
		{
			buffer.append(it.next().getFieldName());
			if(it.hasNext())
				buffer.append(",");
		}

		String queryClause = buffer.toString();
		buffer.setLength(0);

		buffer.append("SELECT ");
		buffer.append(queryClause);
		buffer.append(" FROM ");
		buffer.append(uniqueConstraint.getTableName());

		int counter = 0;
		try
		{
			Statement stmt = this.connection.createStatement();
			ResultSet rs = stmt.executeQuery(buffer.toString());

			while(rs.next())
			{
				StringBuilder pkBuffer = new StringBuilder();

				for(int i = 0; i < uniqueConstraint.getFields().size(); i++)
				{
					if(i == 0)
						pkBuffer.append(rs.getObject(i + 1).toString());
					else
					{
						pkBuffer.append("_");
						pkBuffer.append(rs.getObject(i + 1).toString());
					}
				}

				String unique = pkBuffer.toString();

				UniqueValue uniqueValue = new UniqueValue(uniqueConstraint.getConstraintIdentifier(), unique);
				req.addToUniqueValues(uniqueValue);
				counter++;

				if(counter == 200)
				{
					CoordinatorResponse response = this.ezkClient.sendRequest(req);
					if(!response.isSuccess())
						RuntimeUtils.throwRunTimeException("failed to lock nodes", ExitCode.DUPLICATED_FIELD);

					req.setUniqueValues(new ArrayList<UniqueValue>());
					counter = 0;
				}
			}

			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(rs);

		} catch(SQLException e)
		{
			RuntimeUtils.throwRunTimeException(e.getMessage(), ExitCode.FETCH_RESULTS_ERROR);
		}

		if(LOG.isTraceEnabled())
			LOG.trace("{} values already in use for constraint {}", counter,
					uniqueConstraint.getConstraintIdentifier());
	}

	private void treatAutoIncrementConstraint(AutoIncrementConstraint autoIncrementConstraint)
	{
		DataField field = autoIncrementConstraint.getFields().get(0);
		StringBuilder buffer = new StringBuilder();
		buffer.append("SELECT MAX(");
		buffer.append(field.getFieldName());
		buffer.append(") AS ");
		buffer.append(field.getFieldName());
		buffer.append(" FROM ");
		buffer.append(field.getTableName());

		int maxId = 0;
		try
		{
			Statement stmt = this.connection.createStatement();
			ResultSet rs = stmt.executeQuery(buffer.toString());

			if(rs.next())
			{
				maxId = rs.getInt(field.getFieldName());
				String nodePath = EZKCoordinationExtension.COUNTERS_DIR + File.separatorChar +
						autoIncrementConstraint.getConstraintIdentifier();
				this.zookeeper.create(nodePath, toBytes(maxId), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(rs);

		} catch(SQLException e)
		{
			LOG.error("could not fetch the last id for constraint {}",
					autoIncrementConstraint.getConstraintIdentifier(), e);
			RuntimeUtils.throwRunTimeException(e.getMessage(), ExitCode.ID_GENERATOR_ERROR);
		} catch(InterruptedException e)
		{
			e.printStackTrace();
		} catch(KeeperException e)
		{
			LOG.error(e.getMessage());
			RuntimeUtils.throwRunTimeException(e.getMessage(), ExitCode.INVALIDUSAGE);
		}

		if(LOG.isTraceEnabled())
			LOG.trace("current id for field {} is {}", field.getFieldName(), maxId);
	}

	private void createNode(String path)
	{
		try
		{
			this.zookeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch(KeeperException | InterruptedException e)
		{
			RuntimeUtils.throwRunTimeException(e.getMessage(), ExitCode.NOINITIALIZATION);
		}
	}

	public void exitGracefully() throws InterruptedException
	{
		DbUtils.closeQuietly(this.connection);
		this.ezkClient.closeExtension();
	}

	private static byte[] toBytes(int value)
	{
		byte[] bytes = new byte[4];
		ByteBuffer.wrap(bytes).putInt(value);
		return bytes;
	}

}
