package weaql.client.execution;


import weaql.client.operation.*;
import weaql.common.database.Record;
import weaql.common.database.constraints.fk.ForeignKeyConstraint;
import weaql.common.database.constraints.fk.ParentChildRelation;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.ExecutionPolicy;
import weaql.common.thrift.CRDTPreCompiledOperation;
import weaql.common.util.defaults.DatabaseDefaults;

import java.util.*;


/**
 * Created by dnlopes on 06/12/15.
 */
public class CRDTOperationGenerator
{

	public static String[] generateCrdtOperations(SQLWriteOperation op, String clock, TransactionContext context)
	{
		Record record = op.getRecord();

		switch(op.getOpType())
		{
		case INSERT:
			if(op.getDbTable().isParentTable())
				return insertRow(op.getRecord(), clock, context);
			else
				return insertChildRow(record, clock, context);
		case UPDATE:
			if(op.getDbTable().isParentTable())
				return updateRow(record, clock, context);
			else
				return updateChildRow(record, clock, context);
		case DELETE:
			if(op.getDbTable().isParentTable())
				return deleteParentRow(record, clock, context);
			else
				return deleteRow(record, clock, context);
		}

		return null;
	}

	public static String[] insertRow(Record record, String clock, TransactionContext context)
	{
		record.addData(DatabaseDefaults.DELETED_COLUMN, DatabaseDefaults.NOT_DELETED_VALUE);
		record.addData(DatabaseDefaults.CONTENT_CLOCK_COLUMN, clock);
		record.addData(DatabaseDefaults.DELETED_CLOCK_COLUMN, clock);

		String insertOp = OperationsGenerator.generateInsertOperation(record, context);

		String[] ops = new String[1];
		ops[0] = insertOp;
                System.out.println("****Inside CRDT Operations generator inserrow"+insertOp);
		return ops;
	}

	public static String[] insertChildRow(Record record, String clock, TransactionContext context)
	{
		DatabaseTable dbTable = record.getDatabaseTable();

		if(!dbTable.hasDeletableParents())
			return insertRow(record, clock, context);
		else
		{
			List<String> ops = new LinkedList<>();

			List<ForeignKeyConstraint> fkConstraints = dbTable.getFkConstraints();

			for(ForeignKeyConstraint fkConstraint : fkConstraints)
			{
				if(!fkConstraint.getParentTable().getTablePolicy().allowDeletes())
					continue;

				if(fkConstraint.getPolicy().getExecutionPolicy() == ExecutionPolicy.UPDATEWINS)
				{
					List<ParentChildRelation> fieldsRelations = fkConstraint.getFieldsRelations();

					StringBuilder buffer = new StringBuilder();

					Iterator<ParentChildRelation> it = fieldsRelations.iterator();

					while(it.hasNext())
					{
						ParentChildRelation aRelation = it.next();

						buffer.append(aRelation.getParent().getFieldName());
						buffer.append("=");
						buffer.append(record.getData(aRelation.getChild().getFieldName()));

						if(it.hasNext())
							buffer.append(" AND ");
					}

					String whereClause = buffer.toString();

					String parentVisible = OperationsGenerator.generateSetParentVisible(fkConstraint, whereClause,
							clock, context);
					ops.add(parentVisible);

					String mergedClockOp = OperationsGenerator.mergeDeletedClock(whereClause,
							fkConstraint.getParentTable().getName(), clock, context);
					ops.add(mergedClockOp);
                                        System.out.println("****Inside CRDT Operations generator insertChildRow mergedClockOp: "+mergedClockOp);
				}
			}

			record.addData(DatabaseDefaults.DELETED_COLUMN, DatabaseDefaults.DELETED_VALUE);
			record.addData(DatabaseDefaults.CONTENT_CLOCK_COLUMN, clock);
			record.addData(DatabaseDefaults.DELETED_CLOCK_COLUMN, clock);
			String insertOp = OperationsGenerator.generateInsertOperation(record, context);
			ops.add(insertOp);
			//TODO set visible

			String[] statementsArray = new String[ops.size()];
			statementsArray = ops.toArray(statementsArray);
                        
			return statementsArray;
		}
	}

	public static String[] updateRow(Record record, String clock, TransactionContext context)
	{
		DatabaseTable dbTable = record.getDatabaseTable();
		List<String> statements = new ArrayList<>();
		String recordPrimaryKeyClause = record.getPkValue().getPrimaryKeyWhereClause();
		boolean shouldWriteLWWFields = record.touchedLwwField();

		Map<String, String> lwwFieldsMap = new HashMap<>();
		Map<String, String> deltaFieldsMap = new HashMap<>();

		for(Map.Entry<String, String> newField : record.getRecordData().entrySet())
		{
			DataField field = dbTable.getField(newField.getKey());

			if(field.isPrimaryKey() || field.isImmutableField())
				continue;

			if(field.isDeltaField())
				deltaFieldsMap.put(newField.getKey(), newField.getValue());
			else
			{
				if(shouldWriteLWWFields)
					lwwFieldsMap.put(newField.getKey(), newField.getValue());
			}
		}

		if(lwwFieldsMap.size() > 0)
		{
			String lwwOp = OperationsGenerator.generateUpdateStatement(dbTable, recordPrimaryKeyClause, lwwFieldsMap,
					true, clock, context);
			statements.add(lwwOp);
                        System.out.println("****Inside CRDT Operations generator updaterow lwwOp: "+lwwOp);
		}

		if(deltaFieldsMap.size() > 0)
		{
			String deltasOp = OperationsGenerator.generateUpdateStatement(dbTable, recordPrimaryKeyClause,
					deltaFieldsMap, false, clock, context);
			statements.add(deltasOp);
                        System.out.println("****Inside CRDT Operations generator insertChildRow deltasOp: "+deltasOp);
                          
		}

		String mergeCClockStatement = OperationsGenerator.mergeContentClock(recordPrimaryKeyClause, dbTable.getName(),
				clock, context);
		statements.add(mergeCClockStatement);

		// if @UPDATEWINS, make sure that this row is visible in case some concurrent operation deleted it
		if(dbTable.getExecutionPolicy() == ExecutionPolicy.UPDATEWINS && dbTable.getTablePolicy().allowDeletes())
		{
			String insertRowBack = OperationsGenerator.generateInsertRowBack(recordPrimaryKeyClause, dbTable.getName(),
					clock, context);
			String mergeClockStatement = OperationsGenerator.mergeDeletedClock(recordPrimaryKeyClause,
					dbTable.getName(), clock, context);

			statements.add(insertRowBack);
			statements.add(mergeClockStatement);
                        System.out.println("****Inside CRDT Operations generator insertChildRow mergeClockStatement: "+mergeClockStatement);
		}

		String[] statementsArray = new String[statements.size()];
		statementsArray = statements.toArray(statementsArray);

		return statementsArray;
	}

	public static String[] updateChildRow(Record record, String clock, TransactionContext context)
	{
		DatabaseTable dbTable = record.getDatabaseTable();

		if(!dbTable.hasDeletableParents())
			return updateRow(record, clock, context);
		else
		{
			List<String> ops = new LinkedList<>();

			List<ForeignKeyConstraint> fkConstraints = dbTable.getFkConstraints();

			for(ForeignKeyConstraint fkConstraint : fkConstraints)
			{
				if(!fkConstraint.getParentTable().getTablePolicy().allowDeletes())
					continue;

				if(fkConstraint.getPolicy().getExecutionPolicy() == ExecutionPolicy.UPDATEWINS)
				{
					List<ParentChildRelation> fieldsRelations = fkConstraint.getFieldsRelations();

					StringBuilder buffer = new StringBuilder();

					Iterator<ParentChildRelation> it = fieldsRelations.iterator();

					while(it.hasNext())
					{
						ParentChildRelation aRelation = it.next();

						buffer.append(aRelation.getParent().getFieldName());
						buffer.append("=");
						buffer.append(record.getData(aRelation.getChild().getFieldName()));

						if(it.hasNext())
							buffer.append(" AND ");
					}

					String whereClause = buffer.toString();

					String parentVisible = OperationsGenerator.generateSetParentVisible(fkConstraint, whereClause,
							clock, context);
					ops.add(parentVisible);

					String mergedClockOp = OperationsGenerator.mergeDeletedClock(whereClause,
							fkConstraint.getParentTable().getName(), clock, context);
					ops.add(mergedClockOp);
                                        System.out.println("****Inside CRDT Operations generator insertChildRow mergedClockOp: "+mergedClockOp);
				}
			}

			record.addData(DatabaseDefaults.DELETED_COLUMN, DatabaseDefaults.DELETED_VALUE);

			ops.addAll(Arrays.asList(updateRow(record, clock, context)));

			//TODO generate conditional set visible
			String[] statementsArray = new String[ops.size()];
			statementsArray = ops.toArray(statementsArray);

			return statementsArray;
		}
	}

	public static String[] deleteRow(Record record, String clock, TransactionContext context)
	{
		DatabaseTable dbTable = record.getDatabaseTable();
		String recordPrimaryKeyClause = record.getPkValue().getPrimaryKeyWhereClause();

		ExecutionPolicy tablePolicy = dbTable.getExecutionPolicy();

		String[] ops = new String[1];

		if(tablePolicy == ExecutionPolicy.DELETEWINS)
			ops[0] = OperationsGenerator.generateDeleteDeleteWins(recordPrimaryKeyClause, dbTable.getName(), clock);
		else
			ops[0] = OperationsGenerator.generateDeleteUpdateWins(recordPrimaryKeyClause, dbTable.getName(), clock);
                System.out.println("****Inside CRDT Operations generator deleterow mergedClockOp: "+ops[0]);
		return ops;

	}

	public static String[] deleteParentRow(Record record, String clock, TransactionContext context)
	{
		//TODO implement delete parent-row
		return deleteRow(record, clock, context);
	}

	private static class OperationsGenerator
	{

		private static String UPDATE = "UPDATE ";
		private static String PARENT_VALUES_PARENT = ") VALUES (";
		private static String INSERT_INTO = "INSERT INTO ";
		private static String WHERE = " WHERE ";
		private static String AND = " AND ";
		private static String SET = " SET ";

		private static String SET_DELETED = " SET _del=1 ";
		private static String SET_NOT_DELETED = " SET _del=0 ";

		private static String MERGE_DCLOCK_OP_PREFIX = " SET _dclock=maxClock(_dclock,\'";
		private static String MERGE_CCLOCK_OP_PREFIX = " SET _cclock=maxClock(_cclock,\'";
		private static String DELETE_ROW_OP_SUFFIX_UPDATE_WINS = "isStrictlyGreater(_cclock,\'";
		private static String DELETE_ROW_OP_SUFFIX_DELETE_WINS = "isConcurrentOrGreaterClock(_cclock,\'";
		private static String VISIBLE_PARENT_OP_SUFFIX = "isConcurrentOrGreaterClock(_dclock,\'";
		private static String IS_CONCURRENT_OR_GREATER_DCLOCK = "isConcurrentOrGreaterClock(_dclock,\'";
		private static String CLOCK_IS_GREATER_SUFIX = "clockIsGreater(_cclock,\'";

		public static String generateInsertOperation(Record record, TransactionContext context)
		{
			CRDTPreCompiledOperation op = new CRDTPreCompiledOperation();
			op.setIsCompiled(false);

			StringBuilder buffer = new StringBuilder();
			StringBuilder valuesBuffer = new StringBuilder();

			buffer.append(INSERT_INTO);
			buffer.append(record.getDatabaseTable().getName());
			buffer.append(" (");

			for(Map.Entry<String, String> entry : record.getRecordData().entrySet())
			{
				buffer.append(entry.getKey());
				buffer.append(",");

				if(record.containsSymbolForField(entry.getKey()))
					valuesBuffer.append(record.getSymbolForField(entry.getKey()));
				else
				{
					DataField dataField = record.getDatabaseTable().getField(entry.getKey());
					valuesBuffer.append(dataField.formatValue(entry.getValue()));
				}
				valuesBuffer.append(",");
			}

			if(buffer.charAt(buffer.length() - 1) == ',')
				buffer.setLength(buffer.length() - 1);
			if(valuesBuffer.charAt(valuesBuffer.length() - 1) == ',')
				valuesBuffer.setLength(valuesBuffer.length() - 1);

			buffer.append(PARENT_VALUES_PARENT);
			buffer.append(valuesBuffer.toString());
			buffer.append(")");

			String sqlString = buffer.toString();

			for(String symbol : record.getAllUsedSymbols())
				op.addToSymbols(symbol);

			op.setSqlOp(sqlString);
			context.getPreCompiledTxn().addToOpsList(op);

			return sqlString;
		}

		public static String generateSetParentVisible(ForeignKeyConstraint constraint, String whereClause,
													  String newClock, TransactionContext context)
		{
			CRDTPreCompiledOperation op = new CRDTPreCompiledOperation();
			op.setIsCompiled(false);

			StringBuilder buffer = new StringBuilder();

			buffer.append(UPDATE);
			buffer.append(constraint.getParentTable().getName());
			buffer.append(SET_NOT_DELETED);
			buffer.append(WHERE);
			buffer.append(whereClause);
			buffer.append(AND);
			buffer.append(VISIBLE_PARENT_OP_SUFFIX);
			buffer.append(newClock);
			buffer.append("')=TRUE");

			String sqlString = buffer.toString();

			op.setSqlOp(sqlString);
			context.getPreCompiledTxn().addToOpsList(op);

			return sqlString;
		}

		public static String mergeDeletedClock(String whereClause, String tableName, String newClock,
											   TransactionContext context)
		{
			CRDTPreCompiledOperation op = new CRDTPreCompiledOperation();
			StringBuilder buffer = new StringBuilder();
			op.setIsCompiled(false);

			buffer.append(UPDATE);
			buffer.append(tableName);
			buffer.append(MERGE_DCLOCK_OP_PREFIX);
			buffer.append(newClock);
			buffer.append("\')");
			buffer.append(WHERE);
			buffer.append(whereClause);

			String sqlString = buffer.toString();
			op.setSqlOp(sqlString);
			context.getPreCompiledTxn().addToOpsList(op);

			return sqlString;
		}

		public static String mergeContentClock(String whereClause, String tableName, String newClock,
											   TransactionContext context)
		{
			StringBuilder buffer = new StringBuilder();
			CRDTPreCompiledOperation op = new CRDTPreCompiledOperation();
			op.setIsCompiled(false);

			buffer.append(UPDATE);
			buffer.append(tableName);
			buffer.append(MERGE_CCLOCK_OP_PREFIX);
			buffer.append(newClock);
			buffer.append("\')");
			buffer.append(WHERE);
			buffer.append(whereClause);

			String sqlString = buffer.toString();
			op.setSqlOp(sqlString);
			context.getPreCompiledTxn().addToOpsList(op);

			return sqlString;
		}

		public static String generateUpdateStatement(DatabaseTable table, String rowPkWhereClause,
													 Map<String, String> newFieldsValuesMap, boolean needsWhereClause,
													 String newClock, TransactionContext context)
		{
			StringBuilder buffer = new StringBuilder();
			CRDTPreCompiledOperation op = new CRDTPreCompiledOperation();
			op.setIsCompiled(false);

			buffer.append(UPDATE);
			buffer.append(table.getName());
			buffer.append(SET);

			for(Map.Entry<String, String> entry : newFieldsValuesMap.entrySet())
			{
				DataField dataField = table.getField(entry.getKey());

				if(dataField.isPrimaryKey())
					continue;

				buffer.append(entry.getKey());
				buffer.append("=");
				buffer.append(dataField.formatValue(entry.getValue()));
				buffer.append(",");
			}

			if(buffer.charAt(buffer.length() - 1) == ',')
				buffer.setLength(buffer.length() - 1);

			buffer.append(WHERE);
			buffer.append(rowPkWhereClause);

			if(needsWhereClause)
			{
				buffer.append(AND);
				buffer.append(CLOCK_IS_GREATER_SUFIX);
				buffer.append(newClock);
				buffer.append("\')=TRUE");
			}

			String sqlString = buffer.toString();

			op.setSqlOp(sqlString);
			context.getPreCompiledTxn().addToOpsList(op);

			return sqlString;
		}

		public static String generateInsertRowBack(String rowWhereClause, String tableName, String newClock,
												   TransactionContext context)
		{
			StringBuilder buffer = new StringBuilder();
			CRDTPreCompiledOperation op = new CRDTPreCompiledOperation();
			op.setIsCompiled(false);

			buffer.append(generateSetVisible(rowWhereClause, tableName, context));
			buffer.append(AND);
			buffer.append(IS_CONCURRENT_OR_GREATER_DCLOCK);
			buffer.append(newClock);
			buffer.append("\')=TRUE");

			String sqlString = buffer.toString();

			op.setSqlOp(sqlString);
			context.getPreCompiledTxn().addToOpsList(op);

			return sqlString;
		}

		private static String generateSetVisible(String rowWhereClause, String tableName, TransactionContext context)
		{
			StringBuilder buffer = new StringBuilder();
			CRDTPreCompiledOperation op = new CRDTPreCompiledOperation();
			op.setIsCompiled(false);

			buffer.append(UPDATE);
			buffer.append(tableName);
			buffer.append(SET_NOT_DELETED);
			buffer.append(WHERE);
			buffer.append(rowWhereClause);

			String sqlString = buffer.toString();

			op.setSqlOp(sqlString);
			context.getPreCompiledTxn().addToOpsList(op);

			return sqlString;
		}

		private static String[] generateParentsVisibilityOperations(Map<String, String> parentsMap,
																	DatabaseTable childTable, String clock,
																	TransactionContext context)
		{
			String[] ops = new String[parentsMap.size() * 2];
			int i = 0;

			for(Map.Entry<String, String> parentEntry : parentsMap.entrySet())
			{
				ForeignKeyConstraint foreignKeyConstraint = (ForeignKeyConstraint) childTable.getConstraint(
						parentEntry.getKey());

				DatabaseTable parentTable = foreignKeyConstraint.getParentTable();

				if(!parentTable.getTablePolicy().allowDeletes())
					continue;

				if(foreignKeyConstraint.getPolicy().getExecutionPolicy() == ExecutionPolicy.UPDATEWINS)
				{
					String parentWhereClause = parentEntry.getValue();

					String parentVisible = OperationsGenerator.generateSetParentVisible(foreignKeyConstraint,
							parentWhereClause, clock, context);
					ops[i++] = parentVisible;
					String mergedClockOp = OperationsGenerator.mergeDeletedClock(parentWhereClause,
							foreignKeyConstraint.getParentTable().getName(), clock, context);
					ops[i++] = mergedClockOp;
				}
			}

			return ops;
		}

		//****************************************************************************************//
		//********************************* DELETE OPs
		//****************************************************************************************//

		public static String generateDeleteUpdateWins(String rowWhereClause, String tableName, String newClock)
		{
			StringBuilder buffer = new StringBuilder();
			buffer.append(UPDATE);
			buffer.append(tableName);
			buffer.append(SET_DELETED);
			buffer.append(WHERE);
			buffer.append(rowWhereClause);
			buffer.append(AND);
			buffer.append(DELETE_ROW_OP_SUFFIX_UPDATE_WINS);
			buffer.append(newClock);
			buffer.append("')=TRUE");

			return buffer.toString();
		}

		public static String generateDeleteDeleteWins(String rowWhereClause, String tableName, String newClock)
		{
			StringBuilder buffer = new StringBuilder();
			buffer.append(UPDATE);
			buffer.append(tableName);
			buffer.append(SET_DELETED);
			buffer.append(WHERE);
			buffer.append(rowWhereClause);
			buffer.append(AND);
			buffer.append(DELETE_ROW_OP_SUFFIX_DELETE_WINS);
			buffer.append(newClock);
			buffer.append("')=TRUE");

			return buffer.toString();
		}

	}

}
