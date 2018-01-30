package weaql.common.database.util;


import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.value.FieldValue;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by dnlopes on 28/03/15.
 */
public final class PrimaryKeyValue
{

	private static final String DEFAULT_VALUE = "TRUE";
	private final DatabaseTable table;
	private Map<String, FieldValue> values;
	private String uniqueValue;
	private String primaryKeyWhereClause;
	private String pkValue;
	private boolean isUniqueGenerated;
	private boolean isPkGenerated;
	private boolean isValueGenerated;
	private int missingPksFields;
	private Map<String, DataField> pkFields;


	public PrimaryKeyValue(DatabaseTable table)
	{
		this.table = table;
                if(table!=null && table.getPrimaryKey()!=null && table.getPrimaryKey().getPrimaryKeyFields()!=null)
                    this.pkFields = table.getPrimaryKey().getPrimaryKeyFields();
		this.values = new HashMap<>();

		if(pkFields!=null && values!=null)
                    missingPksFields = pkFields.size() - values.size();

		this.isUniqueGenerated = false;
		this.isPkGenerated = false;
		this.isValueGenerated = false;
	}

	private PrimaryKeyValue(DatabaseTable table, Map<String, FieldValue> values)
	{
		this.table = table;
		this.pkFields = table.getPrimaryKey().getPrimaryKeyFields();
		this.values = values;

		missingPksFields = pkFields.size() - values.size();
		if(missingPksFields == 0)
			preparePrimaryKey();
	}

	public PrimaryKeyValue duplicate()
	{
		return new PrimaryKeyValue(table, new HashMap<>(values));
	}

	public String getUniqueValue()
	{
		if(!isUniqueGenerated)
			preparePrimaryKey();

		return this.uniqueValue;
	}

	public void addFieldValue(FieldValue fieldValue)
	{
		values.put(fieldValue.getDataField().getFieldName(), fieldValue);
		missingPksFields--;
		isUniqueGenerated = false;

		if(missingPksFields == 0)
			preparePrimaryKey();
	}

	public String getPrimaryKeyWhereClause()
	{
		if(this.values.size() == 0)
			return DEFAULT_VALUE;

		if(!isPkGenerated)
			preparePrimaryKey();

		return this.primaryKeyWhereClause;
	}

	public String getValue()
	{
		if(!isValueGenerated)
			preparePrimaryKey();

		return this.pkValue;
	}

	public void preparePrimaryKey()
	{
		if(!this.isValueGenerated)
		{
			generateValue();
			this.isValueGenerated = true;
		}

		if(!this.isUniqueGenerated)
		{
			generateUniqueIdentifier();
			this.isUniqueGenerated = true;
		}

		if(!isPkGenerated)
		{
			generatePkWhereClause();
			this.isPkGenerated = true;
		}
	}

	public FieldValue getFieldValue(String fieldName)
	{
		return this.values.get(fieldName);
	}

	public String getTableName()
	{
		return this.table.getName();
	}

	@Override
	public int hashCode()
	{
		return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
				append(this.uniqueValue).
				toHashCode();
	}

	public void setValues(Map<String, FieldValue> values)
	{
		this.values = values;
	}

	@Override
	public boolean equals(Object obj)
	{
		if(!(obj instanceof PrimaryKeyValue))
			return false;
		if(obj == this)
			return true;

		PrimaryKeyValue otherObject = (PrimaryKeyValue) obj;
		return new EqualsBuilder().
				append(this.uniqueValue, otherObject.getUniqueValue()).
				isEquals();
	}

	public boolean isPrimaryKeyReady()
	{
		return missingPksFields == 0;
	}

	private void generateUniqueIdentifier()
	{
		StringBuilder buffer = new StringBuilder(getTableName());
		buffer.append(":");

		Iterator<FieldValue> it = this.values.values().iterator();
		while(it.hasNext())
		{
			FieldValue fValue = it.next();
			buffer.append(fValue.getFormattedValue());
			if(it.hasNext())
				buffer.append(",");
		}

		this.uniqueValue = buffer.toString();
	}

	private void generatePkWhereClause()
	{
		StringBuilder buffer = new StringBuilder();

		Iterator<FieldValue> it = values.values().iterator();
		while(it.hasNext())
		{
			FieldValue fValue = it.next();
			buffer.append(fValue.getDataField().getFieldName());
			buffer.append("=");
			buffer.append(fValue.getFormattedValue());
			if(it.hasNext())
				buffer.append(" AND ");
		}

		this.isPkGenerated = true;
		this.primaryKeyWhereClause = buffer.toString();
	}

	private void generateValue()
	{
		StringBuilder buffer = new StringBuilder();

		Iterator<FieldValue> it = values.values().iterator();
		while(it.hasNext())
		{
			FieldValue fValue = it.next();
			buffer.append(fValue.getFormattedValue());
			if(it.hasNext())
				buffer.append(",");
		}

		this.pkValue = buffer.toString();
	}

}
