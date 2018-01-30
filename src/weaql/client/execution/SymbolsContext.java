package weaql.client.execution;


import weaql.common.database.Record;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Created by dnlopes on 07/12/15.
 */
public class SymbolsContext
{

	//map symbol -> value
	private Map<String, String> symbolsValuesMapping;
	// maps symbol -> list of records using that symbol
	private Map<String,Set<Record>> symbolsRecordsMapping;
	private int nextTempId;

	public SymbolsContext()
	{
		this.nextTempId = 299999;
		this.symbolsValuesMapping = new HashMap<>();
		this.symbolsRecordsMapping = new HashMap<>();
	}

	public String createIdForSymbol(String symbol)
	{
		String newTempId = String.valueOf(this.nextTempId++);
		this.symbolsValuesMapping.put(symbol, newTempId);
		this.symbolsRecordsMapping.put(symbol, new HashSet<Record>());
		return newTempId;
	}

	public void linkRecordWithSymbol(Record record, String symbol)
	{
		this.symbolsRecordsMapping.get(symbol).add(record);
	}

	public String getSymbolValue(String symbol)
	{
		return this.symbolsValuesMapping.get(symbol);
	}

	public void clear()
	{
		this.symbolsValuesMapping.clear();
		this.symbolsRecordsMapping.clear();
		this.nextTempId = 299999;
	}

	public boolean containsSymbol(String symbol)
	{
		return this.symbolsValuesMapping.containsKey(symbol);
	}

}
