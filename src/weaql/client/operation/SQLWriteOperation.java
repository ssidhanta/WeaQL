package weaql.client.operation;


import weaql.common.database.Record;
import weaql.common.database.util.PrimaryKey;
import weaql.common.database.util.PrimaryKeyValue;
import net.sf.jsqlparser.schema.Table;


/**
 * Created by dnlopes on 06/12/15.
 */
public abstract class SQLWriteOperation extends SQLOperation
{

	protected final PrimaryKey pk;
	protected Record record;
	protected boolean isPrimaryKeySet;

	public SQLWriteOperation(SQLOperationType type, Table table)
	{
		super(type, table);
                //System.out.println("**SQLWriteOperation dbTable name:="+dbTable.getName());
                if(dbTable!=null && dbTable.getPrimaryKey()!=null)
                    this.pk = dbTable.getPrimaryKey();
                else
                     this.pk = null;
                //System.out.println("**SQLWriteOperation dbTable name:="+dbTable.getPrimaryKey());
		this.isPrimaryKeySet = false;
	}

	public abstract void prepareOperation(boolean useWhere, String tempTableName);

	public abstract void prepareForNextInput();

	public abstract void addRecordValue(String column, String value);

	public PrimaryKey getPk()
	{
		return pk;
	}

	public Record getRecord()
	{
		return record;
	}

	public boolean isPrimaryKeySet()
	{
		return isPrimaryKeySet;
	}

	public void setPrimaryKey(PrimaryKeyValue pkValue)
	{
		record.setPkValue(pkValue);
		isPrimaryKeySet = true;
	}
}
