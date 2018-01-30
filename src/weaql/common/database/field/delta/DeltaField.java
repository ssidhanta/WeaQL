package weaql.common.database.field.delta;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.field.DataField;
import weaql.common.database.util.SemanticPolicy;


/**
 * Created by dnlopes on 22/12/15.
 */
public abstract class DeltaField extends DataField
{
	
	protected DeltaField(CRDTFieldType fieldTag, String name, String tableName, String fieldType, boolean isPrimaryKey,
						 boolean isAutoIncremental, int pos, SemanticPolicy semanticPolicy)
	{
		super(fieldTag, name, tableName, fieldType, isPrimaryKey, isAutoIncremental, pos, semanticPolicy);
	}

	public boolean isDeltaField()
	{
		return true;
	}

	public boolean isLwwField()
	{
		return false;
	}

	public boolean isMetadataField()
	{
		return false;
	}

	public boolean isImmutableField()
	{
		return false;
	}

	public boolean isNumberField()
	{
		return true;
	}
}
