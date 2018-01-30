package weaql.common.database.field.delta;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;


public class DeltaInteger extends DeltaField
{

	public DeltaInteger(String dFN, String tN, String dT, boolean iPK, boolean iAIC, int position,
						SemanticPolicy policy)
	{
		super(CRDTFieldType.NUMDELTAINTEGER, dFN, tN, dT, iPK, iAIC, position, policy);

	}

	@Override
	public String formatValue(String Value)
	{

		return Value;
	}

}
