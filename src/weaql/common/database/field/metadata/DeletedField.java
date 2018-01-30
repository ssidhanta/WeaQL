package weaql.common.database.field.metadata;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;
import weaql.common.util.defaults.DatabaseDefaults;


public class DeletedField extends MetadataField
{

	private static final String FIELD_NAME = DatabaseDefaults.DELETED_COLUMN;
	private static final String FIELD_TYPE = "boolean";

	public DeletedField(String tableName, int position)
	{
		super(CRDTFieldType.LWWDELETEDFLAG, FIELD_NAME, tableName, FIELD_TYPE, false, false, position,
				SemanticPolicy.NOSEMANTIC);
	}

	@Override
	public String formatValue(String Value)
	{
		return Value;
	}

	public String getDefaultValue()
	{
		return "false";
	}

}
