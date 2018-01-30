package weaql.server.agents.coordination;


import weaql.common.database.constraints.unique.AutoIncrementConstraint;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.nodes.NodeConfig;
import weaql.common.util.*;
import weaql.common.util.exception.InitComponentFailureException;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.util.TransactionCompilationException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by dnlopes on 24/03/15.
 */
public class IDsManager
{

	private static final Logger LOG = LoggerFactory.getLogger(IDsManager.class);
	private static String PREFIX;

	private Map<String, IDGenerator> idsGenerators;

	public IDsManager(String prefix, NodeConfig replicatorConfig)
	{
		PREFIX = prefix;
		this.idsGenerators = new HashMap<>();

		setup(replicatorConfig);
	}

	public int getNextId(String tableName, String fieldName) throws TransactionCompilationException
	{
		String key = tableName + "_" + fieldName;

		if(!this.idsGenerators.containsKey(key))
			throw new TransactionCompilationException("id generator not found for key: " + key);

		int nextId = this.idsGenerators.get(key).getNextId();

		if(LOG.isTraceEnabled())
			LOG.trace("new unique id generated for key {}: {}", key, nextId);

		return nextId;
	}

	private void setup(NodeConfig config)
	{
		if(LOG.isTraceEnabled())
			LOG.trace("bootstraping id generators for auto increment fields");

		for(DatabaseTable table : WeaQLEnvironment.DB_METADATA.getAllTables())
		{
			for(AutoIncrementConstraint autoIncrementConstraint : table.getAutoIncrementConstraints())
				if(!autoIncrementConstraint.requiresCoordination())
					createIdGenerator(autoIncrementConstraint.getAutoIncrementField(), config);
		}
	}

	private void createIdGenerator(DataField field, NodeConfig config)
	{
		String key = field.getTableName() + "_" + field.getFieldName();

		if(this.idsGenerators.containsKey(key))
		{
			LOG.warn("ids generator already created.");
			return;
		}
				
		try {
			
			IDGenerator newGenerator = new IDGenerator(field, config, Topology.getInstance().getReplicatorsCount());		
			this.idsGenerators.put(key, newGenerator);
			LOG.trace("id generator for field {} created. Initial value {}", field.getFieldName(),
					newGenerator.getCurrentValue());

		} catch (InitComponentFailureException e) {
						
			LOG.warn("id generator for field {} failed. Reason: {}", field.getFieldName(),
					e.getMessage());
		}
	}

	private class IDGenerator
	{

		private final int delta;
		private AtomicInteger currentValue;
		private DataField field;

		public IDGenerator(DataField field, NodeConfig config, int delta) throws InitComponentFailureException
		{
			this.field = field;
			this.currentValue = new AtomicInteger();
			this.delta = delta;

			this.setupGenerator(config);
		}

		private void setupGenerator(NodeConfig config) throws InitComponentFailureException
		{
			StringBuilder buffer = new StringBuilder();
			buffer.append("SELECT MAX(");
			buffer.append(this.field.getFieldName());
			buffer.append(") AS ");
			buffer.append(this.field.getFieldName());
			buffer.append(" FROM ");
			buffer.append(this.field.getTableName());

			Connection tempConnection = null;
			Statement stmt = null;
			ResultSet rs = null;
			try
			{
				tempConnection = ConnectionFactory.getDefaultConnection(config);
				stmt = tempConnection.createStatement();
				rs = stmt.executeQuery(buffer.toString());

				if(rs.next())
				{
					int lastId = rs.getInt(this.field.getFieldName());
					this.currentValue.set(lastId + config.getId());
				} else
				{
					throw new InitComponentFailureException("failed to setup ids generator: could not fetch the last" +
							" " +
							"id for field " + field.getFieldName());
				}
			} catch(SQLException e)
			{
				throw new InitComponentFailureException("failed to setup ids generator: " + e.getMessage());
			} finally
			{
				DbUtils.closeQuietly(tempConnection, stmt, rs);
			}
		}

		public int getNextId()
		{
			int newValue = this.currentValue.addAndGet(this.delta);
			LOG.debug("new id generated for field {}: {}", this.field.getFieldName(), newValue);

			return newValue;
		}

		public int getCurrentValue()
		{
			return this.currentValue.get();
		}
	}
}
