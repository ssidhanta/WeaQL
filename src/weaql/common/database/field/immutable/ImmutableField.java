package weaql.common.database.field.immutable;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.field.DataField;
import weaql.common.database.util.SemanticPolicy;


/**
 * Created by dnlopes on 22/12/15.
 */
public abstract class ImmutableField extends DataField
{
	
	protected ImmutableField(CRDTFieldType fieldTag, String name, String tableName, String fieldType,
							 boolean isPrimaryKey, boolean isAutoIncremental, int pos, SemanticPolicy semanticPolicy)
	{
		super(fieldTag, name, tableName, fieldType, isPrimaryKey, isAutoIncremental, pos, semanticPolicy);
	}

	public boolean isDeltaField()
	{
		return false;
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
		return true;
	}
}
