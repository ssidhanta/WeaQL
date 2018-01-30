package weaql.client.execution.temporary.scratchpad.agent;


import weaql.client.execution.QueryCreator;
import weaql.client.execution.TransactionContext;
import weaql.client.operation.*;
import weaql.client.execution.temporary.scratchpad.IDBScratchpad;
import weaql.client.execution.temporary.scratchpad.ScratchpadException;
import weaql.common.database.Record;
import weaql.common.database.SQLInterface;
import weaql.common.database.constraints.fk.ForeignKeyConstraint;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.DatabaseCommon;
import weaql.common.database.util.PrimaryKeyValue;
import weaql.common.database.util.Row;
import org.apache.commons.dbutils.DbUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import weaql.common.database.constraints.fk.ParentChildRelation;
import weaql.common.database.util.ExecutionPolicy;


/**
 * Created by dnlopes on 04/12/15. Modified by Subhajit on 06/09/17
 */
public class ExecutorAgent extends AbstractExecAgent implements IExecutorAgent
{

	private ExecutionHelper helper;

	public ExecutorAgent(int scratchpadId, int tableId, String tableName, SQLInterface sqlInterface, IDBScratchpad pad,
						 TransactionContext txnRecord) throws SQLException
	{
		super(scratchpadId, tableId, tableName, sqlInterface, pad, txnRecord);

		this.helper = new ExecutionHelper();
	}

	@Override
	public ResultSet executeTemporaryQuery(SQLSelect selectOp) throws ScratchpadException
	{
		long start = System.nanoTime();
		//TODO filter UPDATED ROWS properly
		selectOp.prepareOperation(tempTableName);

		ResultSet rs;
		try
		{
			rs = this.sqlInterface.executeQuery(selectOp.getSQLString());
		} catch(SQLException e)
		{
			throw new ScratchpadException(e.getMessage());
		}

		long estimated = System.nanoTime() - start;
		this.txnRecord.addSelectTime(estimated);
		return rs;
	}

	@Override
	public int executeTemporaryUpdate(SQLWriteOperation sqlOp) throws ScratchpadException
	{
		if(sqlOp.getOpType() == SQLOperationType.DELETE)
		{
			return executeTempOpDelete((SQLDelete) sqlOp);
		} else
		{
			if(sqlOp.getOpType() == SQLOperationType.INSERT)
			{
				isDirty = true;
				return executeTempOpInsert((SQLInsert) sqlOp);
			} else if(sqlOp.getOpType() == SQLOperationType.UPDATE)
			{
				isDirty = true;
				return executeTempOpUpdate((SQLUpdate) sqlOp);
			} else
				throw new ScratchpadException("update statement not found");
		}
	}

	@Override
	public void scanTemporaryTables(List<Record> recordsList) throws ScratchpadException
	{
		StringBuilder buffer = new StringBuilder(FULL_SCAN_PREFIX);
		buffer.append(tempTableName);

		String sqlQuery = buffer.toString();

		ResultSet rs;
		try
		{
			rs = sqlInterface.executeQuery(sqlQuery);

			while(rs.next())
			{
				Record aRecord = DatabaseCommon.loadRecordFromResultSet(rs, databaseTable);
				recordsList.add(aRecord);
			}
		} catch(SQLException e)
		{
			throw new ScratchpadException(e.getMessage());
		}

	}

	private int executeTempOpInsert(SQLInsert insertOp) throws ScratchpadException
	{
		try
		{
			long start = System.nanoTime();
                       
			Record toInsertRecord = insertOp.getRecord();
			
			insertOp.prepareOperation(false, this.tempTableName);
                        //System.out.println("***In ExecAgent executeTempOpInsert before executeUpdate insertOp: "+insertOp.getSQLString());
                         int result = this.sqlInterface.executeUpdate(insertOp.getSQLString());
                        
                        //Added  by Subhajit
                        List<ForeignKeyConstraint> fkConstraints =  insertOp.getDbTable().getFkConstraints();
                        Map<ForeignKeyConstraint,ExecutionPolicy> fkPolicies = new HashMap<>();
                        for(ForeignKeyConstraint fkConstraint : fkConstraints)
			{
                            DatabaseTable parentTable= fkConstraint.getParentTable();
                            List<ParentChildRelation> fieldsRelations = fkConstraint.getFieldsRelations();
                            StringBuilder buffer = new StringBuilder();
                            buffer.append("where ");
                            StringBuilder buffer1 = new StringBuilder();
                            buffer1.append("where ");
                            Iterator<ParentChildRelation> it = fieldsRelations.iterator();
                            fkPolicies.put(fkConstraint, ExecutionPolicy.TC);
                            while(it.hasNext())
			    {
				ParentChildRelation aRelation = it.next();
                                    
				buffer.append(aRelation.getParent().getFieldName());
				buffer.append("=");
				buffer.append(toInsertRecord.getData(aRelation.getChild().getFieldName()));
                                
                                buffer1.append(aRelation.getChild().getFieldName());
				buffer1.append("=");
				buffer1.append(toInsertRecord.getData(aRelation.getChild().getFieldName()));

				if(it.hasNext()){
                                    buffer.append(" AND ");
                                    buffer1.append(" AND ");
                                }
			    }

			    String whereClause = buffer.toString();
                            String whereClause1 = buffer1.toString();
                            StringBuilder sqlBuffer = new StringBuilder("UPDATE ").append(parentTable.getName());
                            sqlBuffer.append(new StringBuilder(" SET `status` = 'T' ")).append(whereClause);
                            int updTStat = this.sqlInterface.executeUpdate(sqlBuffer.toString());
                            toInsertRecord.setStatus(ExecutionPolicy.T.toString());//fkConstraint.getParentTable().setForeignKeyPolicies(fkPolicies);
                            /*StringBuilder sqlBufferRec;
                            sqlBufferRec = new StringBuilder("SELECT status ").append(parentTable.getName()).append(whereClause);*/
                            //sqlBufferRec.append(new StringBuilder(" status = T ")).append(whereClause);
                            //ResultSet resStatus = this.sqlInterface.executeQuery(sqlBufferRec.toString());
                            //toInsertRecord.setStatus(resStatus.getString(0));
                            toInsertRecord.getDatabaseTable().setForeignKeyPolicies(fkPolicies);
                            scratchpad.getWriteSet().addToUpdates(toInsertRecord);
                            scratchpad.getWriteSet().addToUpdates(toInsertRecord);
                            //StringBuilder sqlBuffer1 = new StringBuilder("UPDATE ").append(fkConstraint.getChildTable().getName());
                            //sqlBuffer1.append(new StringBuilder(" SET `status` = 'TC' ")).append(whereClause1);
                            
                            String sqlTC = "INSERT INTO cnflct_flgs (c_tbl_chld, c_tbl_chld_pk, c_tbl_flgs) VALUES ('"+fkConstraint.getChildTable().getName()+"','"+ toInsertRecord.getPkValue().getValue()+"', 'TC')";
                            //System.out.println("*** Inside EzecutorAgent file executeTempOpInsert method sqlTC: "+sqlTC);    
                            int updTStat1 = this.sqlInterface.executeUpdate(sqlTC);
                            
                             //System.out.println("****In ExecutorAgent executeTempOpInsert scratchpad getWriteSet addToUpdates"+toInsertRecord.getPkValue().getValue()); 
                        }
                        
			long estimated = System.nanoTime() - start;
			this.txnRecord.addInsertTime(estimated);
			return result;
		} catch(SQLException e)
		{
                        //System.out.println("***In ExecAgent executeTempOpInsert in catch insertOp: "+insertOp.getSQLString()+" Error messsage: "+e.getMessage()+" SQLState: "+e.getSQLState());
                        e.printStackTrace();
			throw new ScratchpadException(e.getMessage());
		}
	}

	private int executeTempOpDelete(SQLDelete deleteOp) throws ScratchpadException
	{
		Record toDeleteRecord = deleteOp.getRecord();
                if(!toDeleteRecord.isPrimaryKeyReady())
			throw new ScratchpadException("pk value missing for this delete query");

		try
		{
			long start = System.nanoTime();
			StringBuilder buffer = new StringBuilder();
			buffer.append("DELETE FROM ").append(tempTableName).append(" WHERE ");
			buffer.append(deleteOp.getRecord().getPkValue());
			String delete = buffer.toString();
                        int result = this.sqlInterface.executeUpdate(delete);
                        
                        //Added  by Subhajit
                        List<ForeignKeyConstraint> fkConstraints =  deleteOp.getDbTable().getFkConstraints();
                        Map<ForeignKeyConstraint,ExecutionPolicy> fkPolicies = new HashMap<>();
                        for(ForeignKeyConstraint fkConstraint : fkConstraints)
			{
                            List<ParentChildRelation> fieldsRelations = fkConstraint.getFieldsRelations();
                            StringBuilder buffer1 = new StringBuilder();
                            buffer1.append("where ");
                            Iterator<ParentChildRelation> it = fieldsRelations.iterator();
                            fkPolicies.put(fkConstraint, ExecutionPolicy.DC);
                            //toDeleteRecord.getDatabaseTable().setForeignKeyPolicies();
			    while(it.hasNext())
			    {
				ParentChildRelation aRelation = it.next();

				
                                buffer1.append(aRelation.getChild().getFieldName());
				buffer1.append("=");
				buffer1.append(deleteOp.getRecord().getData(aRelation.getChild().getFieldName()));

				if(it.hasNext()){
                                    buffer1.append(" AND ");
                                }
			    }

                            
                            String whereClause1 = buffer1.toString();
                            //StringBuilder sqlBuffer1 = new StringBuilder("UPDATE ").append(fkConstraint.getChildTable().getName());
                            //sqlBuffer1.append(new StringBuilder(" SET `status` = 'DC' ")).append(whereClause1);
                            String sqlDC = "INSERT INTO cnflct_flgs (c_tbl_chld, c_tbl_chld_pk, c_tbl_flgs) VALUES ('"+fkConstraint.getChildTable().getName()+"','"+ deleteOp.getRecord().getPkValue().getValue() +"', 'TC')";
                            //System.out.println("*** Executior agent execInsertOp sqlBuffer1.toString(): "+sqlBuffer1.toString());
                            //System.out.println("*** Inside EzecutorAgent file executeTempDelete method updTStat1: "+sqlDC);
                            int updTStat1 = this.sqlInterface.executeUpdate(sqlDC);
                            
                            //int updTStat1 = this.sqlInterface.executeUpdate(sqlBuffer1.toString());
                        }
                        /*StringBuilder sqlBufferRec;
                        sqlBufferRec = new StringBuilder("SELECT status ").append(tempTableName);
                            //sqlBufferRec.append(new StringBuilder(" status = T ")).append(whereClause);
                        ResultSet resStatus = this.sqlInterface.executeQuery(sqlBufferRec.toString());
                        toDeleteRecord.setStatus(resStatus.getString(0));*/
                        toDeleteRecord.setStatus(ExecutionPolicy.D.toString());
                        
			long estimated = System.nanoTime() - start;
			txnRecord.addDeleteTime(estimated);
			toDeleteRecord.getDatabaseTable().setForeignKeyPolicies(fkPolicies);
                        scratchpad.getWriteSet().addToDeletes(toDeleteRecord);
                        //System.out.println("****In ExecutorAgent executeTempOpDelete scratchpad getWriteSet addToDeletes"+toDeleteRecord.getPkValue().getValue()); 
			return result;
		} catch(SQLException e)
		{
                        e.printStackTrace();
			throw new ScratchpadException(e.getMessage());
		}
	}

	private int executeTempOpUpdate(SQLUpdate updateOp) throws ScratchpadException
	{
		long loadingFromMain;
		long execUpdate;
                        
		Record cachedRecord = updateOp.getCachedRecord();
                Record toUpdateRecord = updateOp.getRecord();
		if(!cachedRecord.isPrimaryKeyReady())
			throw new ScratchpadException("cached record is missing pk value");

		// if NOT in cache, we have to retrieved it from main storage
		// previously inserted records go to cache as well
		if(!scratchpad.getWriteSet().getCachedRecords().containsKey(cachedRecord.getPkValue().getUniqueValue()))
		{
                        //System.out.println("****In ExecutorAgent executeTempOpUpdate !scratchpad.getWriteSet().getCachedRecords().containsKey"); 
			long start = System.nanoTime();
			this.helper.addMissingRowsToScratchpad(updateOp);
			loadingFromMain = System.nanoTime() - start;
			txnRecord.addLoadfromMainTime(loadingFromMain);
		}

		try
		{
			long start = System.nanoTime();
			updateOp.prepareOperation(true, this.tempTableName);
                        int result = this.sqlInterface.executeUpdate(updateOp.getSQLString());
                        //Added  by Subhajit
                        List<ForeignKeyConstraint> fkConstraints =  updateOp.getDbTable().getFkConstraints();
                        //Map<ForeignKeyConstraint,ExecutionPolicy> fkPolicies = new HashMap<>();
                        for(ForeignKeyConstraint fkConstraint : fkConstraints)
			{
                            DatabaseTable parentTable= fkConstraint.getParentTable();
                            List<ParentChildRelation> fieldsRelations = fkConstraint.getFieldsRelations();
                            StringBuilder buffer = new StringBuilder();
                            buffer.append("where ");
                            StringBuilder buffer1 = new StringBuilder();
                            buffer1.append("where ");
                            Iterator<ParentChildRelation> it = fieldsRelations.iterator();
                            //fkPolicies.put(fkConstraint, ExecutionPolicy.TC);
                            //toUpdateRecord.setStatus(toUpdateRecord.getData(fkConstraint.getParentTable());ExecutionPolicy.T.toString());//getData(fkConstraint.getParentTable().toString());
                            while(it.hasNext())
			    {
				ParentChildRelation aRelation = it.next();
                                buffer.append(aRelation.getParent().getFieldName());
				buffer.append("=");
				buffer.append(updateOp.getRecord().getData(aRelation.getChild().getFieldName()));
                                
                                buffer1.append(aRelation.getChild().getFieldName());
				buffer1.append("=");
				buffer1.append(updateOp.getRecord().getData(aRelation.getChild().getFieldName()));

				if(it.hasNext()){
                                    buffer.append(" AND ");
                                    buffer1.append(" AND ");
                                }
			    }

			    String whereClause = buffer.toString();
                            StringBuilder sqlBuffer = new StringBuilder("UPDATE ").append(parentTable.getName());
                            sqlBuffer.append(new StringBuilder(" SET `status` = 'T' ")).append(whereClause);
                            int updTStat = this.sqlInterface.executeUpdate(sqlBuffer.toString());
                            
                            String whereClause1 = buffer1.toString();
                            //StringBuilder sqlBuffer1 = new StringBuilder("UPDATE ").append(fkConstraint.getChildTable().getName());
                            //sqlBuffer1.append(new StringBuilder(" SET `status` = 'TC' ")).append(whereClause1);
                            //int updTStat1 = this.sqlInterface.executeUpdate(sqlBuffer1.toString());
                            String sqlTC = "INSERT INTO cnflct_flgs (c_tbl_chld, c_tbl_chld_pk, c_tbl_flgs) VALUES ('"+fkConstraint.getChildTable().getName()+"','"+ toUpdateRecord.getPkValue().getValue()+"', 'TC')";
                            //System.out.println("*** Executior agent execInsertOp sqlBuffer1.toString(): "+sqlBuffer1.toString());
                            //System.out.println("*** Inside EzecutorAgent file executeTempOpUpdate method updTStat1: "+sqlTC);
                            int updTStat1 = this.sqlInterface.executeUpdate(sqlTC);
                        }
			execUpdate = System.nanoTime() - start;
			txnRecord.addUpdateTime(execUpdate);
			//toUpdateRecord.getDatabaseTable().setForeignKeyPolicies(fkPolicies);
                        toUpdateRecord.setStatus(ExecutionPolicy.I.toString());
                        scratchpad.getWriteSet().addToUpdates(toUpdateRecord);
                        //System.out.println("****In ExecutorAgent executeTempOpUpdate scratchpad getWriteSet addToUpdates"+toUpdateRecord.getPkValue().getValue());    
			return result;
		} catch(SQLException e)
		{
                        e.printStackTrace();
			throw new ScratchpadException(e.getMessage());
		}
	}

	private class ExecutionHelper
	{

		public static final String WHERE = " WHERE (";
		public static final String AND = " AND (";

		/**
		 * Inserts missing rows in the temporary table and returns the list of rows
		 * This must be done before updating rows in the scratchpad.
		 *
		 * @param updateOp
		 * @param pad
		 *
		 * @throws SQLException
		 */
		private void addMissingRowsToScratchpad(SQLUpdate updateOp) throws ScratchpadException
		{
			//TODO (optimization)
			// if we have already loaded the record previously (during parsing time)
			// we do not need to do the select here, we just need to insert in temp tables
			// this can happen when the original update is not specified by the PK
			// in such case, we perform a select PK to know which records will be updated

			StringBuilder buffer = new StringBuilder("SELECT ");
			buffer.append(updateOp.getDbTable().getNormalFieldsSelection());
			buffer.append(" FROM ").append(updateOp.getDbTable().getName());
			buffer.append(" WHERE ").append(updateOp.getCachedRecord().getPkValue().getPrimaryKeyWhereClause());

			String sqlQuery = buffer.toString();

			ResultSet rs;

			try
			{
				rs = sqlInterface.executeQuery(sqlQuery);
				while(rs.next())
				{
					if(!rs.isLast())
						throw new ScratchpadException("expected only one record, but instead we got more");

					buffer.setLength(0);
					buffer.append("INSERT INTO ");
					buffer.append(tempTableName);
					buffer.append(" (");
					StringBuilder valuesBuffer = new StringBuilder(" VALUES (");

					Iterator<DataField> fieldsIt = fields.values().iterator();
					Record cachedRecord = updateOp.getCachedRecord();

					while(fieldsIt.hasNext())
					{
						DataField field = fieldsIt.next();

						buffer.append(field.getFieldName());
						if(fieldsIt.hasNext())
							buffer.append(",");

						String cachedContent = rs.getString(field.getFieldName());

						if(cachedContent == null)
							cachedContent = "NULL";

						cachedRecord.addData(field.getFieldName(), cachedContent);
						valuesBuffer.append(field.formatValue(cachedContent));

						if(fieldsIt.hasNext())
							valuesBuffer.append(",");
					}

					if(!cachedRecord.isFullyCached())
						throw new SQLException("failed to retrieve full record from main storage");
                                        
					scratchpad.getWriteSet().addToCache(cachedRecord);
                                        //System.out.println("*** IN addMissingRowsToScratchpad scratchpad.getWriteSet().addToCache");
					buffer.append(")");
					buffer.append(valuesBuffer.toString());
					buffer.append(")");
					sqlInterface.executeUpdate(buffer.toString());
				}

			} catch(SQLException e)
			{
				throw new ScratchpadException(e.getMessage());
			}
		}

		private Map<String, String> findParentRows(Row childRow, List<ForeignKeyConstraint> constraints,
												   SQLInterface sqlInterface) throws SQLException
		{
			Map<String, String> parentByConstraint = new HashMap<>();

			for(int i = 0; i < constraints.size(); i++)
			{
				ForeignKeyConstraint c = constraints.get(i);

				if(!c.getParentTable().getTablePolicy().allowDeletes())
					continue;

				Row parent = findParent(childRow, c, sqlInterface);
				parentByConstraint.put(c.getConstraintIdentifier(),
						parent.getPrimaryKeyValue().getPrimaryKeyWhereClause());

				if(parent == null)
					throw new SQLException("parent row not found. Foreing key violated");
			}

			//return null in the case where app never deletes any parent
			if(parentByConstraint.size() == 0)
				return null;
			else
				return parentByConstraint;
		}

		private Row findParent(Row childRow, ForeignKeyConstraint constraint, SQLInterface sqlInterface)
				throws SQLException
		{
			String query = QueryCreator.findParent(childRow, constraint, scratchpadId);

			ResultSet rs = sqlInterface.executeQuery(query);
			if(!rs.isBeforeFirst())
			{
				DbUtils.closeQuietly(rs);
				throw new SQLException("parent row not found. Foreing key violated");
			}

			rs.next();
			DatabaseTable remoteTable = constraint.getParentTable();
			PrimaryKeyValue parentPk = DatabaseCommon.getPrimaryKeyValue(rs, remoteTable);
			DbUtils.closeQuietly(rs);

			return new Row(remoteTable, parentPk);
		}
	}

}
