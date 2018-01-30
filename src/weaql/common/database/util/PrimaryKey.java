package weaql.common.database.util;


import weaql.common.database.field.DataField;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Created by dnlopes on 28/03/15.
 */
public class PrimaryKey
{

	private String queryClause;
	private int pkSize;
	private Map<String, DataField> fields;
	private boolean isGenerated;


	public PrimaryKey()
	{
		this.fields = new HashMap<>();
		this.isGenerated = false;
	}


	public void addField(DataField field)
	{
		this.fields.put(field.getFieldName(), field);
		this.isGenerated = false;
	}

	public Map<String, DataField> getPrimaryKeyFields()
	{
		return this.fields;
	}

	public int getSize()
	{
		return this.pkSize;
	}

	public String getQueryClause()
	{
		if(!this.isGenerated)
			this.generateQueryClause();

		return this.queryClause;
	}

	private void generateQueryClause()
	{
		StringBuilder buffer = new StringBuilder();

		Iterator<DataField> it = this.fields.values().iterator();
		while(it.hasNext())
		{
			buffer.append(it.next().getFieldName());
			if(it.hasNext())
				buffer.append(",");
		}

		this.isGenerated = true;
		this.queryClause = buffer.toString();
	}
	
}
