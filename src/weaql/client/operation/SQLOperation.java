package weaql.client.operation;


import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.DatabaseMetadata;
import weaql.common.util.WeaQLEnvironment;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.io.StringReader;


/**
 * Created by dnlopes on 06/12/15.
 */
public abstract class SQLOperation
{

	protected static final DatabaseMetadata DB_METADATA = WeaQLEnvironment.DB_METADATA;

	protected final SQLOperationType opType;
	protected final DatabaseTable dbTable;
	protected final Table table;
	protected String sqlString;

	public SQLOperation(SQLOperationType type, Table table)
	{
                /*if(table == null)
                    System.out.println("***In if SQL Operation table parameter is null");
                else
                    System.out.println("***In else SQL Operation table parameter is not null. table.getName(): "+table.getName());*/
                this.opType = type;
		this.table = table;
		this.dbTable = DB_METADATA.getTable(table.getName());
                /*if(this.dbTable == null)
                    System.out.println("***this.dbTable null");*/
                //System.out.println("***this.dbTable primarykey:="+this.dbTable.getPrimaryKey());
        }

	public abstract void prepareOperation(String tempTableName);

	public abstract SQLOperation duplicate() throws JSQLParserException;

	public static SQLOperation parseSQLOperation(String sql, CCJSqlParserManager parser) throws JSQLParserException
	{
		Statement sqlStmt = parser.parse(new StringReader(sql));

		if(sqlStmt instanceof Insert)
			return new SQLInsert((Insert) sqlStmt);
		else if(sqlStmt instanceof Update)
			return new SQLUpdate((Update) sqlStmt);
		else if(sqlStmt instanceof Select)
			return new SQLSelect((Select) sqlStmt);
		else if(sqlStmt instanceof Delete)
			return new SQLDelete((Delete) sqlStmt);
		else
			throw new JSQLParserException("unkown sql statement");
	}

	public SQLOperationType getOpType()
	{
		return opType;
	}

	public String getSQLString()
	{
		return sqlString;
	}

	public Table getTable()
	{
		return table;
	}

	public DatabaseTable getDbTable()
	{
		return dbTable;
	}
}
