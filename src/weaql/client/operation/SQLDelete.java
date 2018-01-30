package weaql.client.operation;


import weaql.common.database.Record;
import weaql.common.database.field.DataField;
import weaql.common.util.exception.NotCallableException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.delete.Delete;


/**
 * Created by dnlopes on 06/12/15.
 */
public class SQLDelete extends SQLWriteOperation
{

	private final Delete sqlStat;

	public SQLDelete(Delete sqlStat)
	{
		super(SQLOperationType.DELETE, sqlStat.getTable());

		this.sqlStat = sqlStat;
		this.record = new Record(this.dbTable);
	}

	private SQLDelete(Delete sqlStat, Record record)
	{
		super(SQLOperationType.DELETE, sqlStat.getTable());

		this.sqlStat = sqlStat;
		this.record = record;
	}

	@Override
	public void prepareOperation(boolean useWhere, String tempTableName)
	{
		throw new NotCallableException("SQLOperation.prepareOperation method missing implementation");
	}

	@Override
	public void prepareOperation(String tempTableName)
	{
		throw new NotCallableException("SQLOperation.prepareOperation method missing implementation");
	}

	@Override
	public SQLDelete duplicate() throws JSQLParserException
	{
		return new SQLDelete(sqlStat, record.duplicate());
	}

	@Override
	public void prepareForNextInput()
	{
		throw new NotCallableException("SQLOperation.prepareOperation method missing implementation");
	}

	public boolean isPrimaryKeyMissingFromWhere()
	{
		String whereClause = this.sqlStat.getWhere().toString();

		for(DataField pkField : this.pk.getPrimaryKeyFields().values())
			if(!whereClause.contains(pkField.getFieldName()))
				return true;

		return false;
	}

	@Override
	public void addRecordValue(String column, String value)
	{
		throw new NotCallableException("SQLOperation.prepareOperation method missing implementation");
	}

	public Delete getDelete()
	{
		return this.sqlStat;
	}
}
