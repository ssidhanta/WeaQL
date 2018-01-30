package weaql.common.database.util;


import weaql.common.database.Record;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.value.FieldValue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.*;


/**
 * Created by dnlopes on 11/05/15.
 */
public class DatabaseCommon
{

	final static String[] dataTypeList = {"INT", "FLOAT", "DOUBLE", "BOOL", "BOOLEAN", "DATE", "DATETIME",
			"TIMESTAMP", "CHAR", "VARCHAR", "REAL", "INTEGER", "TEXT", "smallint", "decimal", "tinyint", "bigint",
			"datetime",};

	public static PrimaryKeyValue getPrimaryKeyValue(final ResultSet rs, DatabaseTable dbTable) throws SQLException
	{
		PrimaryKeyValue pkValue = new PrimaryKeyValue(dbTable);
		PrimaryKey pk = dbTable.getPrimaryKey();

		for(DataField field : pk.getPrimaryKeyFields().values())
		{
			String fieldValue = rs.getObject(field.getFieldName()).toString();

			if(fieldValue == null)
				throw new SQLException(("primary key cannot be null"));

			FieldValue fValue = new FieldValue(field, fieldValue);
			pkValue.addFieldValue(fValue);
		}

		pkValue.preparePrimaryKey();
		return pkValue;
	}

	public static Date NOW()
	{
		return new Date();
	}

	public static String CURRENTTIMESTAMP(DateFormat dateFormat)
	{
		return dateFormat.format(NOW());
	}

	/**
	 * Gets the _ data_ type.
	 *
	 * @param defStr
	 * 		the def str
	 *
	 * @return the _ data_ type
	 */
	public static String getDataType(String defStr)
	{
		String[] subStrs = defStr.split(" ");
		for(int i = 0; i < subStrs.length; i++)
		{
			String typeStr = subStrs[i];
			int endIndex = subStrs[i].indexOf("(");
			if(endIndex != -1)
				typeStr = subStrs[i].substring(0, endIndex);
			for(int j = 0; j < dataTypeList.length; j++)
			{
				if(typeStr.toUpperCase().equalsIgnoreCase(dataTypeList[j]))
					return dataTypeList[j];
			}
		}
		return "";
	}

	public static Record loadRecordFromResultSet(ResultSet rs, DatabaseTable table) throws SQLException
	{
		Record record = new Record(table);

		for(DataField field : table.getNormalFields().values())
		{
			String value = rs.getString(field.getFieldName());

			if(value == null)
				value = "NULL";

			record.addData(field.getFieldName(), value);
		}

		return record;
	}

	public static double extractDelta(String stringValue, String fieldName) throws SQLException
	{
		double delta = 0;

		if(stringValue.contains("+"))
		{
			String[] splitted = stringValue.split("\\+");

			for(int i = 0; i < splitted.length; i++)
				splitted[i] = splitted[i].trim();

			if(splitted.length != 2)
				throw new SQLException("malformed delta update field");

			if(fieldName.compareTo(splitted[0]) == 0) //[0] is the fieldName
				delta = Double.parseDouble(splitted[1]);
			else //[1] is the fieldName
				delta = Double.parseDouble(splitted[0]);

		} else if(stringValue.contains("-"))
		{
			String[] splitted = stringValue.split("-");

			for(int i = 0; i < splitted.length; i++)
				splitted[i] = splitted[i].trim();

			if(splitted.length != 2)
				throw new SQLException("malformed delta update field");

			if(fieldName.compareTo(splitted[0]) == 0) //[0] is the fieldName
				delta = Double.parseDouble(splitted[1]);
			else //[1] is the fieldName
				delta = Double.parseDouble(splitted[0]);

			delta *= -1;
		}

		return delta;
	}
}
