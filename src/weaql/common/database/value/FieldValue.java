package weaql.common.database.value;


import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.field.DataField;


/**
 * Created by dnlopes on 11/05/15.
 */
public class FieldValue
{

	protected final DataField dataField;
	protected String value;

	public FieldValue(DataField field, String value)
	{
		this.dataField = field;
		this.value = value.trim().replace("'", "");
	}

	public String getValue()
	{
		return this.value;
	}

	public String getFormattedValue()
	{

		CRDTFieldType fieldType = this.dataField.getCrdtType();

		if(fieldType == CRDTFieldType.LWWDATETIME || fieldType == CRDTFieldType.NORMALDATETIME || fieldType ==
				CRDTFieldType.NUMDELTADATETIME || fieldType == CRDTFieldType.LWWSTRING || fieldType ==
				CRDTFieldType.NORMALSTRING ||
				fieldType == CRDTFieldType.LWWLOGICALTIMESTAMP)
			return "'" + this.value + "'";
		else
			return this.value;
	}

	public void setValue(String newValue)
	{
		this.value = newValue;
	}

	public DataField getDataField()
	{
		return this.dataField;
	}

	@Override
	public String toString()
	{
		return this.value;
	}
}
