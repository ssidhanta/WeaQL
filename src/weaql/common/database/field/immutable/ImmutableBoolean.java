package weaql.common.database.field.immutable;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;


public class ImmutableBoolean extends ImmutableField
{

	public ImmutableBoolean(String dFN, String tN, String dT, boolean iPK, boolean iAIC, int position,
							SemanticPolicy policy)
	{
		super(CRDTFieldType.NORMALBOOLEAN, dFN, tN, dT, iPK, iAIC, position, policy);
	}

	@Override
	public String formatValue(String Value)
	{
		return Value;
	}
}
