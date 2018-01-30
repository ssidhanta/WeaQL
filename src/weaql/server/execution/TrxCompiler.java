package weaql.server.execution;


import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.DatabaseMetadata;
import weaql.common.thrift.CRDTCompiledTransaction;
import weaql.common.thrift.CRDTPreCompiledOperation;
import weaql.common.thrift.CRDTPreCompiledTransaction;
import weaql.common.thrift.SymbolEntry;
import weaql.common.util.WeaQLEnvironment;
import weaql.common.util.exception.InitComponentFailureException;
import org.apache.commons.lang3.StringUtils;
import weaql.server.agents.coordination.IDsManager;
import weaql.server.replicator.Replicator;
import weaql.server.util.LogicalClock;
import weaql.server.util.TransactionCompilationException;

import java.util.Map;
import java.util.Set;


/**
 * Created by dnlopes on 09/12/15.
 */
public class TrxCompiler
{

	private final DatabaseMetadata metadata;
	private IDsManager idsManager;

	public TrxCompiler(Replicator replicator) throws InitComponentFailureException
	{
		metadata = WeaQLEnvironment.DB_METADATA;
		idsManager = new IDsManager(replicator.getPrefix(), replicator.getConfig());

	}

	public CRDTCompiledTransaction compileTrx(CRDTPreCompiledTransaction transaction)
			throws TransactionCompilationException
	{
		CRDTCompiledTransaction compiledTransaction = new CRDTCompiledTransaction();
		compiledTransaction.setIsReady(false);
		compiledTransaction.setTxnClock(transaction.getTxnClock());
		compiledTransaction.setReplicatorId(transaction.getReplicatorId());

		if(transaction.isSetSymbolsMap())
		{
			Map<String, SymbolEntry> symbols = transaction.getSymbolsMap();

			//generate unique values locally
			for(SymbolEntry symbolEntry : symbols.values())
			{
				// ignore symbols already exchanged by the coordinator
				if(symbolEntry.isSetRealValue())
					continue;

				// lets generate a unique value locally
				DatabaseTable dbTable = metadata.getTable(symbolEntry.getTableName());
				DataField dataField = dbTable.getField(symbolEntry.getFieldName());

				if(dataField.isNumberField() && dataField.isAutoIncrement())
					symbolEntry.setRealValue(String.valueOf(
							this.idsManager.getNextId(symbolEntry.getTableName(), symbolEntry.getFieldName())));
				else
					throw new TransactionCompilationException(
							"id generator not found for column " + dataField.getFieldName());
			}
		}

		Map<String, SymbolEntry> symbolsMap = transaction.getSymbolsMap();

		for(CRDTPreCompiledOperation op : transaction.getOpsList())
		{
			replacePlaceHolders(op, symbolsMap, transaction);
			//TODO append prefixs to fields when needed, to ensure records uniqueness
			op.setIsCompiled(true);
			compiledTransaction.addToOps(op.getSqlOp());
		}

		compiledTransaction.setIsReady(true);

		if(compiledTransaction.getOps() == null || compiledTransaction.getOps().size() == 0)
			throw new TransactionCompilationException("transaction operations list is null or have no operations");

		return compiledTransaction;
	}

	private void replacePlaceHolders(CRDTPreCompiledOperation op, Map<String, SymbolEntry> symbolsMap,
									 CRDTPreCompiledTransaction transaction)
	{
		String sqlOp = op.getSqlOp();
		String clockReplacer = transaction.getTxnClock();

		String tempOp = StringUtils.replace(sqlOp, LogicalClock.CLOCK_PLACEHOLDER, clockReplacer);
		op.setSqlOp(tempOp);

		if(op.isSetSymbols())
		{
			Set<String> symbolsSet = op.getSymbols();

			for(String symbol : symbolsSet)
			{
				String realValue = symbolsMap.get(symbol).getRealValue();

				tempOp = StringUtils.replace(tempOp, symbol, realValue);
				op.setSqlOp(tempOp);
			}
		}
	}

}
