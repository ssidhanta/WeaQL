package weaql.client.proxy;


import weaql.client.execution.CRDTOperationGenerator;
import weaql.client.execution.TransactionContext;
import weaql.client.operation.*;
import weaql.client.execution.temporary.DBReadOnlyInterface;
import weaql.client.execution.temporary.ReadOnlyInterface;
import weaql.client.execution.temporary.SQLQueryHijacker;
import weaql.client.execution.temporary.WriteSet;
import weaql.client.execution.temporary.scratchpad.*;
import weaql.client.proxy.log.TransactionLog;
import weaql.client.proxy.log.TransactionLogEntry;
import weaql.client.proxy.network.IProxyNetwork;
import weaql.client.proxy.network.SandboxProxyNetwork;
import weaql.common.database.Record;
import weaql.common.database.SQLBasicInterface;
import weaql.common.database.SQLInterface;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.DatabaseCommon;
import weaql.common.nodes.NodeConfig;
import weaql.common.thrift.CRDTPreCompiledTransaction;
import weaql.common.thrift.Status;
import weaql.common.util.ConnectionFactory;
import weaql.common.util.exception.SocketConnectionException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.server.util.LogicalClock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import weaql.common.database.constraints.fk.ForeignKeyConstraint;
import weaql.common.database.util.ExecutionPolicy;
import weaql.common.util.exception.ConfigurationLoadException;


/**
 * Created by dnlopes on 02/09/15. Modified by Subhajit Sidhanta 30/08/2017
 */
public class SandboxExecutionProxy implements Proxy
{

	private static final Logger LOG = LoggerFactory.getLogger(SandboxExecutionProxy.class);

	private final CCJSqlParserManager parserManager;
	private final int proxyId;
	private final IProxyNetwork network;
	private SQLInterface sqlInterface;
	private boolean readOnly, isRunning;
	private TransactionContext txnContext;
	private ReadOnlyInterface readOnlyInterface;
	private IDBScratchpad scratchpad;
	private List<SQLOperation> operationList;
	private TransactionLog transactionLog;

	public SandboxExecutionProxy(final NodeConfig proxyConfig, int proxyId) throws SQLException
	{
                //System.out.println("*** printSandbox SandboxProxy parameters");
		this.proxyId = proxyId;
		this.network = new SandboxProxyNetwork(proxyConfig);
		this.readOnly = false;
		this.isRunning = false;
		this.operationList = new LinkedList<>();
		this.transactionLog = new TransactionLog();
		this.parserManager = new CCJSqlParserManager();

		try
		{
			this.sqlInterface = new SQLBasicInterface(ConnectionFactory.getDefaultConnection(proxyConfig));
			this.readOnlyInterface = new DBReadOnlyInterface(sqlInterface);
			this.txnContext = new TransactionContext(sqlInterface);
			this.scratchpad = new DBScratchpad(sqlInterface, txnContext);
		} catch(SQLException e)
		{
			throw new SQLException("failed to create scratchpad environment for proxy: " + e.getMessage());
		}
	}

	@Override
	public ResultSet executeQuery(String op) throws SQLException
	{
		//its the first op from this txn
		if(!isRunning)
			start();

		long start = System.nanoTime();

		if(readOnly)
		{
			//TODO filter deleted records in this case
			ResultSet rs = this.readOnlyInterface.executeQuery(op);
			long estimated = System.nanoTime() - start;
			this.txnContext.addSelectTime(estimated);
			return rs;
		} else
		{
			SQLOperation[] preparedOps;

			try
			{
				preparedOps = SQLQueryHijacker.pepareOperation(op, txnContext, parserManager);
                                long estimated = System.nanoTime() - start;
				this.txnContext.addToParsingTime(estimated);
			} catch(JSQLParserException e)
			{
				throw new SQLException(e.getMessage());
			}

			if(preparedOps.length != 1)
				throw new SQLException("unexpected number of select queries");

                        SQLSelect selectSQL = null;
                        if(preparedOps[0].getOpType().compareTo(SQLOperationType.SELECT)==1)
                            selectSQL = (SQLSelect) preparedOps[0];

			if(selectSQL!=null && selectSQL.getOpType()!=null && selectSQL.getOpType() != SQLOperationType.SELECT)
				throw new SQLException("expected query op but instead we got an update");

			ResultSet rs;
			long estimated;
			if(readOnly)
			{
				rs = this.readOnlyInterface.executeQuery(selectSQL);
				estimated = System.nanoTime() - start;
				this.txnContext.addSelectTime(estimated);
			} else // we dont measure select times from non-read only txn here. we do it in the lower layers
				rs = this.scratchpad.executeQuery(selectSQL);

			return rs;
		}
	}

	@Override
	public int executeUpdate(String op) throws SQLException
	{
		//its the first op from this txn
		if(!isRunning)
			start();

		if(readOnly)
			throw new SQLException("update statement not acceptable under readonly mode");

		SQLOperation[] preparedOps;

		try
		{
			long start = System.nanoTime();
			preparedOps = SQLQueryHijacker.pepareOperation(op, txnContext, parserManager);
                        long estimated = System.nanoTime() - start;
			this.txnContext.addToParsingTime(estimated);

		} catch(JSQLParserException e)
		{
                        //System.out.println("*** in SandboExecProxy JSQLParserException");
                        e.printStackTrace();
			throw new SQLException("parser exception");
		}

		int result = 0;

		for(SQLOperation updateOp : preparedOps)
		{
			int counter = this.scratchpad.executeUpdate((SQLWriteOperation) updateOp);
			operationList.add(updateOp);
			result += counter;
		}

		return result;
	}

	@Override
	public TransactionLog getTransactionLog()
	{
		return transactionLog;
	}

	@Override
	public int getProxyId()
	{
		return this.proxyId;
	}

	@Override
	public void abort()
	{
		try
		{
			this.sqlInterface.rollback();
		} catch(SQLException e)
		{
			LOG.warn(e.getMessage());
		}

		end();
	}

	@Override
	public void setReadOnly(boolean readOnly)
	{
		this.readOnly = readOnly;
	}

	@Override
	public void commit() throws SQLException
	{       //System.out.println("****Inside the commit method...");
		long endExec = System.nanoTime();
		this.txnContext.setExecTime(endExec - txnContext.getStartTime());

		// if read-only, just return
		if(readOnly)
		{
			end();
			return;
		}
                   
		CRDTPreCompiledTransaction crdtTxn = prepareToCommit();
                long estimated = System.nanoTime() - endExec;
		txnContext.setPrepareOpTime(estimated);

		long commitStart = System.nanoTime();

		if(!crdtTxn.isSetOpsList())
		{
			estimated = System.nanoTime() - commitStart;
			txnContext.setCommitTime(estimated);
			end();
			return;
		}

		Status status = null;
		try
		{
			status = network.commitOperation(crdtTxn);
		} catch(SocketConnectionException e)
		{
			throw new SQLException(e.getMessage());
		} finally
		{
			estimated = System.nanoTime() - commitStart;
			txnContext.setCommitTime(estimated);
			end();
		}

		if(!status.isSuccess())
			throw new SQLException(status.getError());
	}

	@Override
	public void close() throws SQLException
	{
		commit();
	}
        public String getRule(Record record)
        {
            String rule = "DC<TC<D<I<T";
            if(record.getDatabaseTable().getExecutionPolicy()==ExecutionPolicy.AW)
            {   if(record.getDatabaseTable().getFkConstraints().get(0).getPolicy().toString().equalsIgnoreCase(ExecutionPolicy.FR.toString()))
                    rule = "DC<TC<D<I<T";
                else if(record.getDatabaseTable().getFkConstraints().get(0).getPolicy().toString().equalsIgnoreCase(ExecutionPolicy.IR.toString()))
                     rule = "T<TC<D<I<DC";
            }
            else if(record.getDatabaseTable().getExecutionPolicy()==ExecutionPolicy.RW)
            {   if(record.getDatabaseTable().getFkConstraints().get(0).getPolicy().toString().equalsIgnoreCase(ExecutionPolicy.FR.toString()))
                    rule = "DC<TC<I<D<T";
                else if(record.getDatabaseTable().getFkConstraints().get(0).getPolicy().toString().equalsIgnoreCase(ExecutionPolicy.IR.toString()))
                    rule = "T<TC<I<D<DC";
            }
            return rule;
        }    
        public  String getFkplicy(Record record)
        {
            String fkPolicy = null;
            try {
                
                String sqlTC = "SELECT c_tbl_flgs FROM cnflct_flgs WHERE c_tbl_chld = '"+record.getDatabaseTable().getName() +"' AND c_tbl_chld_pk = "+record.getPkValue().getValue();
                //System.out.println("*** Inside SandboxProxy file getFkplicy sqlTC"+sqlTC);
                ResultSet rs = this.sqlInterface.executeQuery(sqlTC);
                
                if(rs!=null && rs.next())
		{
			fkPolicy = rs.getString(1);
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
               // Logger.getLogger(SandboxExecutionProxy.class.getName()).log(Level.SEVERE, null, ex);
            }
            return fkPolicy;
            
        }
         public List determinePriorities(String rule, Record record, Record record1)
        {
            List retList = new ArrayList();
            String x = "1", tag = "I";
            String[] ruleArr = rule.split("<");
            Map<ForeignKeyConstraint,ExecutionPolicy> fkPolicies = record.getDatabaseTable().getForeignKeyPolicies();
            Map<ForeignKeyConstraint,ExecutionPolicy> fkPolicies1 = record1.getDatabaseTable().getForeignKeyPolicies();
            
           for(int i=0;i<ruleArr.length;i++)
            {
                if(ruleArr[i].equalsIgnoreCase(record.getStatus()))
                 {   tag = ruleArr[i];
                     x = "1";
                 }
                else if(ruleArr[i].equalsIgnoreCase(record1.getStatus()))
                {    tag = ruleArr[i];
                     x = "2";
                }
                
                if(ruleArr[i].equalsIgnoreCase(getFkplicy(record)))
                    {
                        tag = ruleArr[i];
                        x = "1";
                    }
                else if(ruleArr[i].equalsIgnoreCase(getFkplicy(record1)))
                    {
                        tag = ruleArr[i];
                        x = "2";
                    }
                 
//                for(Map.Entry<ForeignKeyConstraint,ExecutionPolicy> fkPolicy : fkPolicies.entrySet())
//                {
//                    if(ruleArr[i].equalsIgnoreCase(fkPolicy.getValue().toString()))
//                    {
//                        tag = ruleArr[i];
//                        x = "1";
//                    }
//                }
//                for(Map.Entry<ForeignKeyConstraint,ExecutionPolicy> fkPolicy : fkPolicies1.entrySet())
//                {
//                    if(ruleArr[i].equalsIgnoreCase(fkPolicy.getValue().toString()))
//                    {   tag = ruleArr[i];
//                        x = "2";
//                    }
//                }        
            }
            retList.add(x);
            retList.add(tag);
            return retList;
        }
	private CRDTPreCompiledTransaction prepareToCommit() throws SQLException
	{
		WriteSet snapshot = scratchpad.getWriteSet();
                
		Map<String, Record> cache = snapshot.getCachedRecords();
		Map<String, Record> updates = snapshot.getUpdates();
		Map<String, Record> inserts = snapshot.getInserts();
		Map<String, Record> deletes = snapshot.getDeletes();
                //System.out.println("****in  the prepareToCommit method before iteration over records...cache.size(): "+cache.size() + " updates.size(): " +updates.size());
            	String clockPlaceHolder = LogicalClock.CLOCK_PLACEHOLLDER_WITH_ESCAPED_CHARS;
                String rule, tag;
                // take care of INSERTS
		for(Record insertedRecord : inserts.values())
		{
			DatabaseTable table = insertedRecord.getDatabaseTable();
			String pkValueString = insertedRecord.getPkValue().getUniqueValue();
                        rule = getRule(insertedRecord);
                        //System.out.println("****in  the prepareToCommit method within iteration over insertedRecord...");
			if(updates.containsKey(pkValueString))
			{
				Record updatedVersion = updates.get(pkValueString);
                                tag = determinePriorities(rule,insertedRecord, updatedVersion).get(0).toString();
                                //System.out.println("*****inside insert records  of Sandbox Exec Proxy");
                                if((tag.equalsIgnoreCase("I") || tag.equalsIgnoreCase("TC") || tag.equalsIgnoreCase("T")) && determinePriorities(rule,insertedRecord, updatedVersion).get(1).toString().equalsIgnoreCase("1"))
                                {   
                                    //insertedRecord.mergeRecords(updatedVersion);
                                    
                                    // it was inserted and later updated.
                                    // use inserted record values as base, and then override
                                    // the columns that are present in the update record
                                    for(Map.Entry<String, String> updatedEntry : updatedVersion.getRecordData().entrySet())
                                    {
                                            DataField field = table.getField(updatedEntry.getKey());

                                            if(field.isPrimaryKey())
                                                    continue;

                                            if(field.isLwwField())
                                                    insertedRecord.addData(updatedEntry.getKey(), updatedEntry.getValue());
                                            else if(field.isDeltaField())
                                            {
                                                    double initValue = Double.parseDouble(insertedRecord.getData(field.getFieldName()));
                                                    double diffValue = DatabaseCommon.extractDelta(updatedVersion.getData(field.getFieldName()),
                                                                    field.getFieldName());

                                                    double finalValue = initValue + diffValue;
                                                    insertedRecord.addData(field.getFieldName(), String.valueOf(finalValue));
                                            }
                                    }
                                }
				// remove from updates for performance
				// we no longer have to execute a update for this record
				updates.remove(updatedVersion.getPkValue().getUniqueValue());
                                if(table.isChildTable())
                                    CRDTOperationGenerator.insertChildRow(insertedRecord, clockPlaceHolder, txnContext);
                                else
                                    CRDTOperationGenerator.insertRow(insertedRecord, clockPlaceHolder, txnContext);
			}
                        else if(deletes.containsKey(pkValueString))
			{
				Record deletedVersion = deletes.get(pkValueString);
                                tag = determinePriorities(rule,insertedRecord, deletedVersion).get(0).toString();
                                if((tag.equalsIgnoreCase("I") || tag.equalsIgnoreCase("TC") || tag.equalsIgnoreCase("T")) && determinePriorities(rule,insertedRecord, deletedVersion).get(1).toString().equalsIgnoreCase("1"))
                                {
                                    if(table.isChildTable())
                                        CRDTOperationGenerator.insertChildRow(insertedRecord, clockPlaceHolder, txnContext);
                                    else
                                        CRDTOperationGenerator.insertRow(insertedRecord, clockPlaceHolder, txnContext);
                                }
                        }
			/*if(table.isChildTable())
				CRDTOperationGenerator.insertChildRow(insertedRecord, clockPlaceHolder, txnContext);
			else
				CRDTOperationGenerator.insertRow(insertedRecord, clockPlaceHolder, txnContext);*/
		}

		for(Record updatedRecord : updates.values())
		{
			DatabaseTable table = updatedRecord.getDatabaseTable();
                        //System.out.println("*****inside update records  of Sandbox Exec Proxy");
			//if(!cache.containsKey(updatedRecord.getPkValue().getUniqueValue()))
			//	throw new SQLException("updated record missing from cache");
                        String pkValueString = updatedRecord.getPkValue().getUniqueValue();
                        Record cachedRecord = cache.get(updatedRecord.getPkValue().getUniqueValue());
			//rule = getRule(cachedRecord);
                        rule = getRule(updatedRecord);
                        if(inserts.containsKey(pkValueString))
			{
				Record insertedVersion = inserts.get(pkValueString);
                                tag = determinePriorities(rule,updatedRecord, insertedVersion).get(0).toString();
                                if((tag.equalsIgnoreCase("I") || tag.equalsIgnoreCase("TC") || tag.equalsIgnoreCase("T")) && determinePriorities(rule,updatedRecord, insertedVersion).get(1).toString().equalsIgnoreCase("1"))
                                {
                                    
                                    // use cached record as baseline, then override the changed columns
                                    // that are present in the updated version of the record
                                    for(Map.Entry<String, String> updatedEntry : updatedRecord.getRecordData().entrySet())
                                    {
                                            DataField field = table.getField(updatedEntry.getKey());

                                            if(field.isPrimaryKey())
                                                    continue;

                                            cachedRecord.addData(updatedEntry.getKey(), updatedEntry.getValue());
                                    }
                                }
                                if(table.isChildTable())
                                    CRDTOperationGenerator.updateChildRow(cachedRecord, clockPlaceHolder, txnContext);
                                else
                                    CRDTOperationGenerator.updateRow(cachedRecord, clockPlaceHolder, txnContext);
                        }
			 else if(deletes.containsKey(pkValueString))
			{
				Record deletedVersion = deletes.get(pkValueString);
                                tag = determinePriorities(rule,cachedRecord, deletedVersion).get(0).toString();
                                if((tag.equalsIgnoreCase("I") || tag.equalsIgnoreCase("TC") || tag.equalsIgnoreCase("T")) && determinePriorities(rule,cachedRecord, deletedVersion).get(1).toString().equalsIgnoreCase("1"))
                                {
                                    if(table.isChildTable())
                                        CRDTOperationGenerator.insertChildRow(cachedRecord, clockPlaceHolder, txnContext);
                                    else
                                        CRDTOperationGenerator.insertRow(cachedRecord, clockPlaceHolder, txnContext);
                                }
                        }
		}
		// take care of DELETES
		for(Record deletedRecord : deletes.values())
		{
			DatabaseTable table = deletedRecord.getDatabaseTable();
                        String pkValueString = deletedRecord.getPkValue().getUniqueValue();
                        //System.out.println("*****inside delete records  of Sandbox Exec Proxy");
                        //Record cachedRecord = cache.get(deletedRecord.getPkValue().getUniqueValue());
			rule = getRule(deletedRecord);
                        if(inserts.containsKey(pkValueString))
			{
				Record insertedVersion = inserts.get(pkValueString);
                                tag = determinePriorities(rule,deletedRecord, insertedVersion).get(0).toString();
                                if((tag.equalsIgnoreCase("D") || tag.equalsIgnoreCase("DC")) && determinePriorities(rule,deletedRecord, insertedVersion).get(1).toString().equalsIgnoreCase("1"))
                                {
                                    if(table.isParentTable())
                                            CRDTOperationGenerator.deleteParentRow(deletedRecord, clockPlaceHolder, txnContext);
                                    else
                                            CRDTOperationGenerator.deleteRow(deletedRecord, clockPlaceHolder, txnContext);
		
                                }
                        }
                        else if(updates.containsKey(pkValueString))
			{
				Record updatedVersion = updates.get(pkValueString);
                                tag = determinePriorities(rule,deletedRecord, updatedVersion).get(0).toString();
                                if((tag.equalsIgnoreCase("D") || tag.equalsIgnoreCase("DC")) && determinePriorities(rule,deletedRecord, updatedVersion).get(1).toString().equalsIgnoreCase("1"))
                                {
                                    if(table.isParentTable())
                                            CRDTOperationGenerator.deleteParentRow(deletedRecord, clockPlaceHolder, txnContext);
                                    else
                                            CRDTOperationGenerator.deleteRow(deletedRecord, clockPlaceHolder, txnContext);
		
                                }
                        }
                        
                }
                //System.out.println("*****before return of Sandbox Exec Proxy");
		return txnContext.getPreCompiledTxn();
	}

	private void start()
	{
		reset();
		txnContext.setStartTime(System.nanoTime());
		isRunning = true;
	}

	private void end()
	{
		txnContext.setEndTime(System.nanoTime());
		isRunning = false;
		TransactionLogEntry entry = new TransactionLogEntry(proxyId, txnContext.getSelectsTime(),
				txnContext.getUpdatesTime(), txnContext.getInsertsTime(), txnContext.getDeletesTime(),
				txnContext.getParsingTime(), txnContext.getExecTime(), txnContext.getCommitTime(),
				txnContext.getPrepareOpTime(), txnContext.getLoadFromMainTime());

		transactionLog.addEntry(entry);
	}

	private void reset()
	{
		txnContext.clear();
		operationList.clear();

		try
		{
			scratchpad.clearScratchpad();
		} catch(SQLException e)
		{
			LOG.warn("failed to clean scratchpad tables");
		}
	}
        public static void main(String[] args) throws ConfigurationLoadException,SQLException
	{
            String rule = "DC<TC<D<I<T", x;
            System.out.println("rule:="+rule.split("<")[0]);
            String[] ruleArr = rule.split("<");
            for(int i=0;i<ruleArr.length;i++)
                x = ruleArr[i];
        }
}
