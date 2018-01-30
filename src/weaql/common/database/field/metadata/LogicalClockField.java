package weaql.common.database.field.metadata;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;


public class LogicalClockField extends MetadataField
{

	private static final String FIELD_TYPE = "string";

	public LogicalClockField(String tableName, int position, String fieldName)
	{
		super(CRDTFieldType.LWWLOGICALTIMESTAMP, fieldName, tableName, FIELD_TYPE, false, false,
				position, SemanticPolicy.NOSEMANTIC);
	}

	@Override
	public String formatValue(String Value)
	{
		if(Value.indexOf("'") == 0 && Value.lastIndexOf("'") == Value.length() - 1)
			return Value;
		return "'" + Value + "'";
	}

	@Override
	public boolean isStringField()
	{
		return true;
	}

}
