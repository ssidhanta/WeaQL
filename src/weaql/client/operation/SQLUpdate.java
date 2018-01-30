package weaql.client.operation;


import weaql.common.database.Record;
import weaql.common.database.field.DataField;
import weaql.common.database.util.PrimaryKeyValue;
import weaql.common.util.exception.NotCallableException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.update.Update;


/**
 * Created by dnlopes on 06/12/15. Modified by Subhajit on 06/09/17
 */
public class SQLUpdate extends SQLWriteOperation
{

	private final Update sqlStat;
	private Record cachedRecord;
	private StringBuilder sqlBuffer;

	public SQLUpdate(Update sqlStat) throws JSQLParserException
	{
		super(SQLOperationType.UPDATE, sqlStat.getTables().get(0));

		if(sqlStat.getTables().size() != 1)
			throw new JSQLParserException("multi-table updates not supported");

		this.sqlStat = sqlStat;
		this.sqlBuffer = new StringBuilder("");
		this.record = new Record(this.dbTable);
		this.cachedRecord = new Record(this.dbTable);
	}

	private SQLUpdate(Update sqlStat, Record record, Record cachedRecord, StringBuilder sqlBuffer) throws
			JSQLParserException
	{
		super(SQLOperationType.UPDATE, sqlStat.getTables().get(0));

		if(sqlStat.getTables().size() != 1)
			throw new JSQLParserException("multi-table updates not supported");

		this.sqlStat = sqlStat;
		this.record = record;
		this.cachedRecord = cachedRecord;
		this.sqlBuffer = sqlBuffer;
	}

	@Override
	public void prepareOperation(boolean useWhere, String tempTableName)
	{
		StringBuilder sqlOpBuffer = new StringBuilder("UPDATE ").append(tempTableName).append(" SET ");
		sqlOpBuffer.append(this.sqlBuffer);
                sqlOpBuffer.append(", `status` = 'I' ");
                //sqlOpBuffer.append(this.sqlBuffer);
		sqlOpBuffer.append(" WHERE ");
		if(useWhere)
			sqlOpBuffer.append(sqlStat.getWhere().toString());
		else
			sqlOpBuffer.append(record.getPkValue().getPrimaryKeyWhereClause());

		this.sqlString = sqlOpBuffer.toString();
	}

	@Override
	public void prepareForNextInput()
	{
		this.sqlBuffer.append(",");
	}

	@Override
	public void addRecordValue(String column, String value)
	{
		this.record.addData(column, value);
		this.sqlBuffer.append(" ").append(column).append("=").append(value);
	}

	public void addRecordPrimaryKeyData(String column, String value)
	{
		this.record.addData(column, value);
	}

	@Override
	public void setPrimaryKey(PrimaryKeyValue pkValue)
	{
		super.setPrimaryKey(pkValue);
		cachedRecord.setPkValue(pkValue);
	}

	public Update getUpdate()
	{
		return sqlStat;
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
	public void prepareOperation(String tempTableName)
	{
		throw new NotCallableException("SQLUpdate.prepareOperation(string) method missing implementation");
	}

	public SQLUpdate duplicate() throws JSQLParserException
	{
		return new SQLUpdate(sqlStat, record.duplicate(), cachedRecord.duplicate(), new StringBuilder(sqlBuffer));
	}

	public Record getCachedRecord()
	{
		return cachedRecord;
	}

	public void setCachedRecord(Record cachedRecord)
	{
		this.cachedRecord = cachedRecord;
	}

}
