package weaql.client.execution.temporary;


import applications.util.SymbolsManager;
import weaql.client.execution.TransactionContext;
import weaql.client.operation.*;
import weaql.common.database.Record;
import weaql.common.database.constraints.unique.UniqueConstraint;
import weaql.common.database.field.DataField;
import weaql.common.database.util.DatabaseCommon;
import weaql.common.thrift.ThriftUtils;
import weaql.common.thrift.UniqueValue;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by dnlopes on 08/12/15.
 */
public final class SQLQueryHijacker
{

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	public static SQLOperation[] pepareOperation(String sqlOpString, TransactionContext context,
												 CCJSqlParserManager parser) throws JSQLParserException, SQLException
	{       //System.out.println("**** inside prepareoperation sqlOpString:="+sqlOpString);
		SQLOperation sqlOp = SQLOperation.parseSQLOperation(sqlOpString, parser);

		if(sqlOp.getOpType() == SQLOperationType.INSERT)
			return prepareInsertOperation((SQLInsert) sqlOp, context);
		else if(sqlOp.getOpType() == SQLOperationType.UPDATE)
			return prepareUpdateOperation((SQLUpdate) sqlOp, context);
		else if(sqlOp.getOpType() == SQLOperationType.DELETE)
			return prepareDeleteOperation((SQLDelete) sqlOp, context);
		else if(sqlOp.getOpType() == SQLOperationType.SELECT)
			return prepareSelectOperation((SQLSelect) sqlOp);
		else
			throw new JSQLParserException("unkown SQL operation type");

	}

	private static SQLOperation[] prepareSelectOperation(SQLSelect selectSQL)
	{
		SQLOperation[] select = new SQLOperation[1];
		select[0] = selectSQL;
		return select;
	}

	private static SQLOperation[] prepareDeleteOperation(SQLDelete deleteSQL, TransactionContext context)
			throws JSQLParserException
	{
		Record toDeleteRecord = deleteSQL.getRecord();

		if(deleteSQL.getDelete().getWhere() != null)
		{
			Map<String, String> whereClauseMap = getFieldsMapFromWhereClause(
					deleteSQL.getDelete().getWhere().toString());

			for(Map.Entry<String, String> entry : whereClauseMap.entrySet())
				toDeleteRecord.addData(entry.getKey(), entry.getValue());
		}

		if(!toDeleteRecord.isPrimaryKeyReady())
		{
			StringBuilder sqlQuery = new StringBuilder("SELECT ");
			sqlQuery.append(deleteSQL.getPk().getQueryClause());
			sqlQuery.append(" FROM ").append(deleteSQL.getDbTable().getName());
			sqlQuery.append(" WHERE ").append(deleteSQL.getDelete().getWhere().toString());
			List<SQLOperation> deleteOps = new LinkedList<>();

			try
			{
				ResultSet rs = context.getSqlInterface().executeQuery(sqlQuery.toString());

				while(rs.next())
				{
					SQLDelete aDelete = deleteSQL.duplicate();
					Record aRecord = aDelete.getRecord();

					for(DataField pkField : deleteSQL.getPk().getPrimaryKeyFields().values())
					{
						String value = rs.getString(pkField.getFieldName()).trim();
						if(value == null)
							value = "NULL";

						aRecord.addData(pkField.getFieldName(), value);
					}

					if(!aRecord.isPrimaryKeyReady())
						throw new SQLException("failed to retrieve pk value from main storage");

					deleteOps.add(aDelete);
				}

			} catch(SQLException e)
			{
				throw new JSQLParserException("failed to retrieve pks values for non-deterministic update");
			}

			return deleteOps.toArray(new SQLOperation[deleteOps.size()]);

		} else
		{
			SQLOperation[] delete = new SQLOperation[1];
			delete[0] = deleteSQL;
			return delete;
		}
	}

	private static SQLOperation[] prepareUpdateOperation(final SQLUpdate updateSQL, TransactionContext context)
			throws JSQLParserException
	{
		Record toUpdateRecord = updateSQL.getRecord();
		Record cachedRecord = updateSQL.getCachedRecord();

		if(updateSQL.getUpdate().getWhere() != null)
		{
			Map<String, String> whereClauseMap = getFieldsMapFromWhereClause(
					updateSQL.getUpdate().getWhere().toString());

			for(Map.Entry<String, String> entry : whereClauseMap.entrySet())
			{
				toUpdateRecord.addData(entry.getKey(), entry.getValue());
				cachedRecord.addData(entry.getKey(), entry.getValue());
			}
		}

		Iterator colIt = updateSQL.getUpdate().getColumns().iterator();
		Iterator valueIt = updateSQL.getUpdate().getExpressions().iterator();

		while(colIt.hasNext())
		{
			String column = colIt.next().toString().trim();
			String value = valueIt.next().toString().trim();

			if(value.equalsIgnoreCase("NOW()") || value.equalsIgnoreCase("NOW") || value.equalsIgnoreCase(
					"CURRENT_TIMESTAMP") || value.equalsIgnoreCase("CURRENT_TIMESTAMP()") || value.equalsIgnoreCase(
					"CURRENT_DATE"))
				value = "'" + DatabaseCommon.CURRENTTIMESTAMP(DATE_FORMAT) + "'";

			if(value.contains("SELECT") || value.contains("select"))
				throw new JSQLParserException("nested select not yet supported");

			updateSQL.addRecordValue(column, value);

			if(colIt.hasNext())
				updateSQL.prepareForNextInput();
		}

		if(!toUpdateRecord.isPrimaryKeyReady())
		{
			//where clause figure out whether this is specify by primary key or not, if yes, go ahead,
			//it not, please first query
			StringBuilder sqlQuery = new StringBuilder("SELECT ");
			sqlQuery.append(updateSQL.getPk().getQueryClause());
			sqlQuery.append(" FROM ").append(updateSQL.getDbTable().getName());
			sqlQuery.append(" WHERE ").append(updateSQL.getUpdate().getWhere().toString());
			List<SQLOperation> updateOps = new LinkedList<>();

			try
			{
				ResultSet rs = context.getSqlInterface().executeQuery(sqlQuery.toString());

				while(rs.next())
				{
					SQLUpdate anUpdate = updateSQL.duplicate();
					Record aCachedRecord = anUpdate.getCachedRecord();

					for(DataField pkField : updateSQL.getPk().getPrimaryKeyFields().values())
					{
						String cachedContent = rs.getString(pkField.getFieldName()).trim();

						if(cachedContent == null)
							cachedContent = "NULL";

						aCachedRecord.addData(pkField.getFieldName(), cachedContent);
					}

					if(!aCachedRecord.isPrimaryKeyReady())
						throw new SQLException("failed to retrieve pk value from main storage for record update");

					updateOps.add(anUpdate);
				}

			} catch(SQLException e)
			{
				throw new JSQLParserException("failed to retrieve pks values for non-deterministic update");
			}

			return updateOps.toArray(new SQLOperation[updateOps.size()]);
		} else
		{
			SQLOperation[] update = new SQLOperation[1];
			update[0] = updateSQL;
			return update;
		}
	}

	private static SQLOperation[] prepareInsertOperation(SQLInsert insertSQL, TransactionContext context)
			throws JSQLParserException, SQLException
	{
		Iterator colIt = insertSQL.getInsert().getColumns().iterator();
		Iterator valueIt = ((ExpressionList) insertSQL.getInsert().getItemsList()).getExpressions().iterator();

		while(colIt.hasNext())
		{
			String column = colIt.next().toString().trim();
			String value = valueIt.next().toString().trim();

			if(value.equalsIgnoreCase("NOW()") || value.equalsIgnoreCase("NOW") || value.equalsIgnoreCase(
					"CURRENT_TIMESTAMP") || value.equalsIgnoreCase("CURRENT_TIMESTAMP()") || value.equalsIgnoreCase(
					"CURRENT_DATE"))
				value = "'" + DatabaseCommon.CURRENTTIMESTAMP(DATE_FORMAT) + "'";

			if(value.contains(SymbolsManager.SYMBOL_PREFIX))
			{
				if(context.getSymbolsManager().containsSymbol(value)) // get tmp value already generated
				{
					insertSQL.getRecord().addSymbolEntry(value, column);
					String tmpId = context.getSymbolsManager().getSymbolValue(value);
					context.getSymbolsManager().linkRecordWithSymbol(insertSQL.getRecord(), value);
					value = tmpId;
				} else // create new entry
				{
					DataField dField = insertSQL.getDbTable().getField(column);
					ThriftUtils.createSymbolEntry(context, value, dField, insertSQL.getDbTable());
					insertSQL.getRecord().addSymbolEntry(value, column);
					String tmpId = context.getSymbolsManager().createIdForSymbol(value);
					context.getSymbolsManager().linkRecordWithSymbol(insertSQL.getRecord(), value);
					value = tmpId;
				}
			}

			insertSQL.addRecordValue(column, value);

			if(colIt.hasNext())
				insertSQL.prepareForNextInput();

			if(value.contains("SELECT") || value.contains("select"))
				throw new SQLException("nested select not yet supported");
		}

		if(insertSQL.isMissingValues())
		{
			Set<DataField> missing = insertSQL.getMissingFields();

			for(DataField dField : missing)
			{
				if(dField.getDefaultFieldValue() != null)
					insertSQL.addRecordValue(dField.getFieldName(), dField.getDefaultFieldValue().getFormattedValue());
				else if(!dField.isAutoIncrement())
					throw new SQLException(
							"only auto_increment fields or that have a default value set can be " + "missing " +
									"from" +
									" " +
									"SQL query");
				else // is auto_increment
				{
					//TODO we must generate a different symbol to exchange in replicator
					insertSQL.addRecordValue(dField.getFieldName(), SymbolsManager.ONE_TIME_SYMBOL);
				}
			}
		}

		if(!insertSQL.getDbTable().isFreeToInsert())
		{
			for(UniqueConstraint uniqueConstraint : insertSQL.getDbTable().getUniqueConstraints())
			{
				if(!uniqueConstraint.requiresCoordination())
					continue;

				List<DataField> fieldsList = uniqueConstraint.getFields();
				StringBuilder uniqueBuffer = new StringBuilder();

				for(DataField aField : fieldsList)
				{
					uniqueBuffer.append(insertSQL.getRecord().getData(aField.getFieldName()));
					uniqueBuffer.append("_");
				}

				uniqueBuffer.setLength(uniqueBuffer.length() - 1);
				UniqueValue uniqueRequest = new UniqueValue();
				uniqueRequest.setConstraintId(uniqueConstraint.getConstraintIdentifier());
				uniqueRequest.setValue(uniqueBuffer.toString());
				context.getCoordinatorRequest().addToUniqueValues(uniqueRequest);
			}
		}

		SQLOperation[] insert = new SQLOperation[1];
		insert[0] = insertSQL;
		return insert;
	}

	private static Map<String, String> getFieldsMapFromWhereClause(String whereClause) throws JSQLParserException
	{
		final Map<String, String> whereClauseMap = new HashMap<>();

		ExpressionVisitorAdapter adapter = new ExpressionVisitorAdapter()
		{

			@Override
			public void visit(EqualsTo expr)
			{
				if(expr.getLeftExpression() instanceof Column)
					whereClauseMap.put(expr.getLeftExpression().toString(), expr.getRightExpression().toString());
				else
					whereClauseMap.put(expr.getRightExpression().toString(), expr.getLeftExpression().toString());
			}
		};

		Expression expr = CCJSqlParserUtil.parseCondExpression(whereClause);
		expr.accept(adapter);
		return whereClauseMap;
	}
}
