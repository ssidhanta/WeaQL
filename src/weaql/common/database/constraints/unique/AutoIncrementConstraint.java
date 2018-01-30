package weaql.common.database.constraints.unique;


import weaql.common.database.constraints.AbstractConstraint;
import weaql.common.database.constraints.ConstraintType;
import weaql.common.database.field.DataField;


/**
 * Created by dnlopes on 24/03/15.
 */
public class AutoIncrementConstraint extends AbstractConstraint
{

	private DataField autoIncrementField;

	public AutoIncrementConstraint(boolean requiresCoordination)
	{
		super(ConstraintType.AUTO_INCREMENT, requiresCoordination);
		this.autoIncrementField = null;
	}

	@Override
	public void addField(DataField field)
	{
		this.fields.add(field);
		this.fieldsMap.put(field.getFieldName(), field);

		if(this.autoIncrementField == null)
			this.autoIncrementField = field;
	}

	public DataField getAutoIncrementField()
	{
		return this.autoIncrementField;
	}
}