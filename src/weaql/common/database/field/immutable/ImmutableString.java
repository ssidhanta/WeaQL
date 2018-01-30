package weaql.common.database.field.immutable;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;


public class ImmutableString extends ImmutableField
{

	public ImmutableString(String dFN, String tN, String dT, boolean iPK, boolean iAIC, int position,
						   SemanticPolicy policy)
	{
		super(CRDTFieldType.NORMALSTRING, dFN, tN, dT, iPK, iAIC, position, policy);
	}

	@Override
	public String formatValue(String Value)
	{
		if((Value.indexOf("'") == 0 && Value.lastIndexOf("'") == Value.length() - 1) || (Value.indexOf(
				"\"") == 0 && Value.lastIndexOf("\"") == Value.length() - 1))
			return Value;
		return "'" + Value + "'";
	}

	@Override
	public boolean isStringField()
	{
		return true;
	}
}
