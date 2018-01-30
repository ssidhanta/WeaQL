package weaql.common.database.field;


import weaql.common.database.util.SemanticPolicy;


public class NoCRDTField extends DataField
{

	public NoCRDTField(String dFN, String tN, String dT, boolean iPK, boolean iAIC, int position, SemanticPolicy
			policy)
	{
		super(CRDTFieldType.NONCRDTFIELD, dFN, tN, dT, iPK, iAIC, position, policy);
	}

	@Override
	public String formatValue(String Value)
	{
		return Value;
	}

	public boolean isMetadataField()
	{
		return false;
	}

	public boolean isDeltaField()
	{
		return false;
	}

	public boolean isLwwField()
	{
		return false;
	}

	public boolean isImmutableField()
	{
		return true;
	}

}
