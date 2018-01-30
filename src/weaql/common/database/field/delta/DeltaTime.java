package weaql.common.database.field.delta;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;


public class DeltaTime extends DeltaField
{

	public DeltaTime(String dFN, String tN, String dT, boolean iPK, boolean iAIC, int position, SemanticPolicy policy)
	{
		super(CRDTFieldType.NUMDELTADOUBLE, dFN, tN, dT, iPK, iAIC, position, policy);
	}

	@Override
	public String formatValue(String Value)
	{
		if((Value.indexOf("'") == 0 && Value.lastIndexOf("'") == Value.length() - 1) || (Value.indexOf(
				"\"") == 0 && Value.lastIndexOf("\"") == Value.length() - 1))
			return Value;
		return "'" + Value + "'";
	}

	public boolean isNumberField()
	{
		return false;
	}

}
