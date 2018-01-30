package weaql.common.database.field.metadata;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.field.DataField;
import weaql.common.database.util.SemanticPolicy;


/**
 * Created by dnlopes on 22/12/15.
 */
public abstract class MetadataField extends DataField
{
	
	protected MetadataField(CRDTFieldType fieldTag, String name, String tableName, String fieldType,
							boolean isPrimaryKey, boolean isAutoIncremental, int pos, SemanticPolicy semanticPolicy)
	{
		super(fieldTag, name, tableName, fieldType, isPrimaryKey, isAutoIncremental, pos, semanticPolicy);
	}

	public boolean isMetadataField()
	{
		return true;
	}

	public boolean isDeltaField()
	{
		return false;
	}

	public boolean isLwwField()
	{
		return true;
	}

	public boolean isImmutableField()
	{
		return false;
	}
}
