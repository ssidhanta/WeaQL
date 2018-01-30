package weaql.common.database.constraints.fk;


import weaql.common.database.field.DataField;


/**
 * Created by dnlopes on 12/05/15.
 * Links a child column with a parent column on a remote table
 */
public class ParentChildRelation
{

	private final DataField parent;
	private final DataField child;

	public ParentChildRelation(DataField parent, DataField child)
	{
		this.parent = parent;
		this.child = child;
	}

	public DataField getParent()
	{
		return this.parent;
	}

	public DataField getChild()
	{
		return this.child;
	}
	
}
