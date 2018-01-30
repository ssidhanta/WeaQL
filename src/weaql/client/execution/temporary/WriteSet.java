package weaql.client.execution.temporary;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.database.Record;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.DatabaseCommon;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by dnlopes on 06/12/15.
 */
public class WriteSet
{

	private static final Logger LOG = LoggerFactory.getLogger(WriteSet.class);

	private Map<String, Record> inserts;
	private Map<String, Record> updates;
	private Map<String, Record> deletes;
	private Map<String, Record> cachedRecords;
	private Map<String, Record> deletedRecords;

	public WriteSet()
	{
		this.inserts = new HashMap<>();
		this.updates = new HashMap<>();
		this.deletes = new HashMap<>();
		this.cachedRecords = new HashMap<>();
		this.deletedRecords = new HashMap<>();
        }

	public void addToCache(Record record)
	{
		String pkValue = record.getPkValue().getUniqueValue();
		cachedRecords.put(pkValue, record);
	}

	public Map<String, Record> getCachedRecords()
	{
                return cachedRecords;
	}

	public void addToInserts(Record record) throws SQLException
	{
		String pkValue = record.getPkValue().getUniqueValue();
		if(inserts.containsKey(pkValue))
			throw new SQLException("duplicated record entry");

		this.inserts.put(pkValue, record);
	}

	public void addToUpdates(Record record)
	{
		String pkValue = record.getPkValue().getUniqueValue();

		if(updates.containsKey(pkValue))
		{
			// record was updated twice, lets merge the values
			DatabaseTable table = record.getDatabaseTable();

			Record oldRecord = updates.get(pkValue);

			for(Map.Entry<String, String> dataEntry : record.getRecordData().entrySet())
			{
				DataField field = table.getField(dataEntry.getKey());

				if(field.isPrimaryKey())
					continue;
				if(field.isLwwField())
					oldRecord.addData(dataEntry.getKey(), dataEntry.getValue());
				else if(field.isDeltaField())
				{
					if(!oldRecord.getRecordData().containsKey(field.getFieldName()))
						oldRecord.addData(dataEntry.getKey(), dataEntry.getValue());
					else
					{
						try
						{
							String oldDeltaString = oldRecord.getData(field.getFieldName());
							String newDeltaString = record.getData(field.getFieldName());
							double oldDelta = DatabaseCommon.extractDelta(oldDeltaString, field.getFieldName());
							double newDelta = DatabaseCommon.extractDelta(newDeltaString, field.getFieldName());

							double updatedDelta = oldDelta + newDelta;
                                                        if(updatedDelta > 0)
								oldRecord.addData(dataEntry.getKey(), field.getFieldName() + "+" + updatedDelta);
							else
								oldRecord.addData(dataEntry.getKey(), field.getFieldName() + "-" + updatedDelta);

						} catch(SQLException e)
						{
							LOG.warn("could not merge delta values", e.getMessage());
						}
					}
				}
			}
		} else
			this.updates.put(pkValue, record);
	}

	public void addToDeletes(Record record)
	{
		String pkValue = record.getPkValue().getUniqueValue();
		deletes.put(pkValue, record);

		if(cachedRecords.containsKey(pkValue))
			cachedRecords.remove(pkValue);
	}

	public Map<String, Record> getInserts()
	{
            return inserts;
	}

	public Map<String, Record> getUpdates()
	{
            return updates;
	}

	public Map<String, Record> getDeletes()
	{
            return deletes;
	}

	public void clear()
	{
		this.inserts.clear();
		this.updates.clear();
		this.deletes.clear();
		this.cachedRecords.clear();
		this.deletedRecords.clear();
	}
	
}
