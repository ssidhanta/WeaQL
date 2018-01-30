package weaql.client.operation;


import weaql.common.database.Record;
import weaql.common.database.field.DataField;
import weaql.common.database.util.PrimaryKey;
import weaql.common.util.exception.NotCallableException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.insert.Insert;

import java.util.*;


/**
 * Created by dnlopes on 06/12/15. Modified by Subhajit on 06/09/17
 */
public class SQLInsert extends SQLWriteOperation
{

	private final Insert sqlStat;
	private StringBuilder columnsBuffer, valuesBuffer;

	public SQLInsert(Insert insertStat)
	{
		super(SQLOperationType.INSERT, insertStat.getTable());

		this.sqlStat = insertStat;
		this.columnsBuffer = new StringBuilder(" (");
		this.valuesBuffer = new StringBuilder("(");
		this.record = new Record(this.dbTable);
	}

	@Override
	public void prepareOperation(boolean useWhere, String tempTableName)
	{
		StringBuilder sqlBuffer = new StringBuilder("INSERT INTO ").append(tempTableName);
		sqlBuffer.append(columnsBuffer);
                sqlBuffer.append(new StringBuilder(", status"));
		sqlBuffer.append(") VALUES ").append(valuesBuffer).append(new StringBuilder(", 'I'")).append(")");

		this.sqlString = sqlBuffer.toString();
	}

	@Override
	public void prepareOperation(String tempTableName)
	{
		throw new NotCallableException("SQLOperation.prepareOperation method missing implementation");
	}

	@Override
	public SQLOperation duplicate() throws JSQLParserException
	{
		throw new NotCallableException("SQLOperation.duplicate method missing implementation");
	}

	@Override
	public void prepareForNextInput()
	{
		this.columnsBuffer.append(",");
		this.valuesBuffer.append(",");
	}

	@Override
	public void addRecordValue(String column, String value)
	{
		this.record.addData(column, value);
		this.columnsBuffer.append(column);
		this.valuesBuffer.append(value);
	}

	public Set<DataField> getMissingFields()
	{
		Set<DataField> missing = new HashSet<>();

		for(DataField field : this.dbTable.getNormalFields().values())
			if(!this.record.containsEntry(field.getFieldName()))
				missing.add(field);

		return missing;
	}

	public boolean isMissingValues()
	{
		//TODO TEMPORARY small hack.
		return false;
		/*
		if(record.getRecordData().size() == this.dbTable.getMandatoryFields())
			return false;
		else
			return true;
			*/
	}

	public Insert getInsert()
	{
		return sqlStat;
	}

	public PrimaryKey getPk()
	{
		return pk;
	}
}
