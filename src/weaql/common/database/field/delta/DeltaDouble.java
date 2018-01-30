package weaql.common.database.field.delta;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;


public class DeltaDouble extends DeltaField
{

	public DeltaDouble(String dFN, String tN, String dT, boolean iPK, boolean iAIC, int position, SemanticPolicy
			policy)
	{
		super(CRDTFieldType.NUMDELTADOUBLE, dFN, tN, dT, iPK, iAIC, position, policy);
	}

	@Override
	public String formatValue(String Value)
	{
		return Value;
	}

}
