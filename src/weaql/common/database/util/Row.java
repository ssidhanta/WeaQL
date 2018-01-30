package weaql.common.database.util;


import weaql.common.database.constraints.Constraint;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.value.FieldValue;

import java.util.*;


/**
 * Created by dnlopes on 11/05/15.
 */
public class Row
{

	private final DatabaseTable table;
	private final PrimaryKeyValue pkValue;
	private Map<String, FieldValue> fieldValues;
	private Map<String, FieldValue> newFieldValues;
	private boolean hasSideEffects;
	private Set<Constraint> contraintsToCheck;

	public Row(DatabaseTable databaseTable, PrimaryKeyValue pkValue)
	{
		this.table = databaseTable;
		this.pkValue = pkValue;
		this.fieldValues = new HashMap<>();
		this.newFieldValues = new HashMap<>();
		this.contraintsToCheck = new HashSet<>();
		this.hasSideEffects = false;
	}

	public void updateFieldValue(FieldValue newValue)
	{
		DataField field = newValue.getDataField();

		this.newFieldValues.put(field.getFieldName(), newValue);

		if(field.hasChilds())
			this.hasSideEffects = true;
	}

	public void addFieldValue(FieldValue value)
	{
		this.fieldValues.put(value.getDataField().getFieldName(), value);
	}

	public FieldValue getFieldValue(String fieldName)
	{
		if(this.newFieldValues.containsKey(fieldName))
			return this.newFieldValues.get(fieldName);
		else
			return this.fieldValues.get(fieldName);
	}

	public FieldValue getUpdateFieldValue(String fieldName)
	{
		return this.newFieldValues.get(fieldName);
	}

	public boolean hasSideEffects()
	{
		return this.hasSideEffects;
	}

	public Collection<FieldValue> getFieldValues()
	{
		return this.fieldValues.values();
	}

	public Map<String, FieldValue> getFieldsValuesMap()
	{
		return this.fieldValues;
	}

	public DatabaseTable getTable()
	{
		return this.table;
	}

	public PrimaryKeyValue getPrimaryKeyValue()
	{
		return this.pkValue;
	}

	public boolean containsNewField(String key)
	{
		return this.newFieldValues.containsKey(key);
	}

	public void mergeUpdates()
	{
		for(Map.Entry<String, FieldValue> entry : this.newFieldValues.entrySet())
			this.fieldValues.put(entry.getKey(), entry.getValue());
	}

	public Set<Constraint> getContraintsToCheck()
	{
		return this.contraintsToCheck;
	}

	public void addConstraintToverify(Constraint c)
	{
		this.contraintsToCheck.add(c);
	}

}
