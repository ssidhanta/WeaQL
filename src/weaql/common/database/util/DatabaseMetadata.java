package weaql.common.database.util;


import weaql.common.database.table.DatabaseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.RuntimeUtils;
import weaql.common.util.ExitCode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by dnlopes on 12/03/15.
 */
public class DatabaseMetadata
{

	private static final Logger LOG = LoggerFactory.getLogger(DatabaseMetadata.class);

	private Map<String, DatabaseTable> tablesMap;

	public DatabaseMetadata()
	{
		tablesMap = new HashMap<>();
	}

	public DatabaseTable getTable(String tableName)
	{   
                /*if(tablesMap.containsKey(tableName))
                    System.out.println("******In DBMetadata class gettable method If condition tablesMap contains tableName: "+tableName);
                 else
                    System.out.println("******In DBMetadata class gettable method Else condition not contains tableName: "+tableName);*/
		return tablesMap.get(tableName);
	}

	public void addTable(DatabaseTable table)
	{
		if(tablesMap.containsKey(table.getName()))
		{
			LOG.error("table {} already exists", table.getName());
			RuntimeUtils.throwRunTimeException("duplicated table in ddl", ExitCode.UNEXPECTED_TABLE);
		}
                //System.out.println("******In DBMetadata class addTable method. tableName: "+table.getName());
		tablesMap.put(table.getName(), table);
	}

	public Collection<DatabaseTable> getAllTables()
	{
		return this.tablesMap.values();
	}

}
