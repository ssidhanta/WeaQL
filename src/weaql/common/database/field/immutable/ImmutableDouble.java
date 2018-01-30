package weaql.common.database.field.immutable;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.util.SemanticPolicy;


/**
 * The Class LWW_DOUBLE.
 */
public class ImmutableDouble extends ImmutableField
{

	public ImmutableDouble(String dFN, String tN, String dT, boolean iPK, boolean iAIC, int position,
						   SemanticPolicy policy)
	{
		super(CRDTFieldType.NORMALDOUBLE, dFN, tN, dT, iPK, iAIC, position, policy);
	}

	@Override
	public String formatValue(String Value)
	{
		return Value;
	}

	@Override
	public boolean isNumberField()
	{
		return true;
	}
}
