package weaql.common.database.constraints;


import weaql.common.database.field.DataField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by dnlopes on 06/03/15.
 */
public abstract class AbstractConstraint implements Constraint
{

	protected List<DataField> fields;
	protected Map<String, DataField> fieldsMap;
	protected ConstraintType constraintType;
	private String constraintIdentifier;
	private String tableName;
	private boolean requiresCoordination;

	public AbstractConstraint(ConstraintType type, boolean requiresCoordination)
	{
		this.constraintType = type;
		this.fields = new ArrayList<>();
		this.fieldsMap = new HashMap<>();
		this.requiresCoordination = requiresCoordination;
	}

	@Override
	public ConstraintType getType()
	{
		return this.constraintType;
	}

	@Override
	public List<DataField> getFields()
	{
		return this.fields;
	}

	@Override
	public void addField(DataField field)
	{
		this.fields.add(field);
		this.fieldsMap.put(field.getFieldName(), field);
	}

	@Override
	public String getConstraintIdentifier()
	{
		return this.constraintIdentifier;
	}

	@Override
	public void setTableName(String name)
	{
		this.tableName = name;
	}

	@Override
	public void generateIdentifier()
	{
		StringBuilder buffer = new StringBuilder();

		for(DataField field : fields)
		{
			buffer.append(field.getFieldName());
			buffer.append("_");
		}

		buffer.append(this.tableName);
		buffer.append("_");
		buffer.append(this.constraintType);
		this.constraintIdentifier = buffer.toString();
	}

	@Override
	public String getTableName()
	{
		return this.tableName;
	}

	@Override
	public boolean constainsField(DataField field)
	{
		return this.fieldsMap.containsKey(field.getFieldName());
	}

	@Override
	public boolean requiresCoordination()
	{
		return this.requiresCoordination;
	}

	@Override
	public void setRequiresCoordination(boolean requiresCoordination)
	{
		this.requiresCoordination = requiresCoordination;
	}

}
