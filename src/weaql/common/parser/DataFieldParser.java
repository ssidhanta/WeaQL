package weaql.common.parser;


import weaql.common.database.field.delta.DeltaTime;
import weaql.common.database.field.delta.DeltaDouble;
import weaql.common.database.field.delta.DeltaFloat;
import weaql.common.database.field.delta.DeltaInteger;
import weaql.common.database.field.immutable.*;
import weaql.common.database.field.lww.*;
import weaql.common.database.util.*;
import weaql.common.database.field.*;
import weaql.common.database.value.NullFieldValue;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import weaql.common.util.RuntimeUtils;
import weaql.common.util.ExitCode;


/**
 * The Class DataFieldParser.
 */
public class DataFieldParser
{

	public static DataField createField(String tableName, String attributeDef, int position)
	{
		attributeDef = attributeDef.trim();

		String[] splitted = splitAnnotationsFromField(attributeDef);
		String annotations = splitted[0];
		String fieldDefinition = splitted[1];

		CRDTFieldType crdtType = getFieldCRDTType(annotations);
		SemanticPolicy semanticPolicy = getSemanticPolicy(annotations);
		String fieldName = getFieldName(fieldDefinition);
		String dataType = getFieldDataType(fieldDefinition);
		boolean isPrimaryKey = isPrimaryKey(attributeDef);
		boolean isAutoIncremantal = isAutoIncremental(attributeDef);

		DataField field = null;
		switch(crdtType)
		{
		case NONCRDTFIELD:
			field = new NoCRDTField(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case LWWINTEGER:
			field = new LwwInteger(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case LWWFLOAT:
			field = new LwwFloat(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case LWWDOUBLE:
			field = new LwwDouble(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case LWWSTRING:
			field = new LwwString(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case LWWDATETIME:
			field = new LwwDate(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case LWWBOOLEAN:
			field = new LwwBoolean(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NUMDELTAINTEGER:
			field = new DeltaInteger(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NUMDELTAFLOAT:
			field = new DeltaFloat(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NUMDELTADOUBLE:
			field = new DeltaDouble(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NUMDELTADATETIME:
			field = new DeltaTime(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NORMALINTEGER:
			field = new ImmutableInteger(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NORMALFLOAT:
			field = new ImmutableFloat(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NORMALDOUBLE:
			field = new ImmutableDouble(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NORMALSTRING:
			field = new ImmutableString(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NORMALDATETIME:
			field = new ImmutableDate(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
			break;
		case NORMALBOOLEAN:
			field = new ImmutableBoolean(fieldName, tableName, dataType, isPrimaryKey, isAutoIncremantal, position,
					semanticPolicy);
		default:
			RuntimeUtils.throwRunTimeException("unknown CRDT data type:" + crdtType, ExitCode.SCHEMANOCRDTTABLE);
		}

		setDefaultValue(field, fieldDefinition);
		return field;
	}

	private static CRDTFieldType getFieldCRDTType(String annotations)
	{
		int numberOfAnnotations = StringUtils.countMatches(annotations, "@");

		if(numberOfAnnotations == 0) // no annotations, use default
			return CRDTFieldType.NONCRDTFIELD;
		else if(numberOfAnnotations == 1)
		{
			String annotation = StringUtils.substring(annotations, 1);
			if(EnumUtils.isValidEnum(CRDTFieldType.class, annotation))
				return CRDTFieldType.valueOf(StringUtils.substring(annotations, 1));
			else
				return CRDTFieldType.NONCRDTFIELD;
		} else
		{
			String[] splittedAnnotations = StringUtils.split(annotations, " ");
			if(EnumUtils.isValidEnum(CRDTFieldType.class, StringUtils.substring(splittedAnnotations[0], 1)))
				return CRDTFieldType.valueOf(StringUtils.substring(splittedAnnotations[0], 1));
			else if(EnumUtils.isValidEnum(CRDTFieldType.class, StringUtils.substring(splittedAnnotations[1], 1)))
				return CRDTFieldType.valueOf(StringUtils.substring(splittedAnnotations[1], 1));
			else
				RuntimeUtils.throwRunTimeException("unexpected annotation for CrdtDataFieldType",
						ExitCode.INVALIDUSAGE);
		}

		return null;
	}

	private static SemanticPolicy getSemanticPolicy(String annotations)
	{
		int numberOfAnnotations = StringUtils.countMatches(annotations, "@");

		if(numberOfAnnotations == 0) // no annotations, use default
			return SemanticPolicy.SEMANTIC;
		else if(numberOfAnnotations == 1)
		{
			String annotation = StringUtils.substring(annotations, 1);
			if(EnumUtils.isValidEnum(SemanticPolicy.class, annotation))
				return SemanticPolicy.valueOf(StringUtils.substring(annotations, 1));
			else
				return SemanticPolicy.SEMANTIC;
		} else
		{
			String[] splittedAnnotations = StringUtils.split(annotations, " ");
			if(EnumUtils.isValidEnum(SemanticPolicy.class, StringUtils.substring(splittedAnnotations[0], 1)))
				return SemanticPolicy.valueOf(StringUtils.substring(splittedAnnotations[0], 1));
			else if(EnumUtils.isValidEnum(SemanticPolicy.class, StringUtils.substring(splittedAnnotations[1], 1)))
				return SemanticPolicy.valueOf(StringUtils.substring(splittedAnnotations[1], 1));
			else
				RuntimeUtils.throwRunTimeException("unexpected annotation for SemanticPolicy", ExitCode.INVALIDUSAGE);
		}

		return null;
	}

	private static String[] splitAnnotationsFromField(String fieldDefinition)
	{
		int numberOfAnnotations = StringUtils.countMatches(fieldDefinition, "@");
		int whiteSpaceIndex = 0;

		if(numberOfAnnotations == 0)
			return new String[]{"", fieldDefinition.trim()};
		else if(numberOfAnnotations == 1)
			whiteSpaceIndex = StringUtils.ordinalIndexOf(fieldDefinition, " ", 1);
		else if(numberOfAnnotations == 2)
			whiteSpaceIndex = StringUtils.ordinalIndexOf(fieldDefinition, " ", 2);
		else
			RuntimeUtils.throwRunTimeException("unexpected number of annotations", ExitCode.SCHEMANOCRDTTABLE);

		String annotations = StringUtils.left(fieldDefinition, whiteSpaceIndex).trim();
		String fieldData = StringUtils.right(fieldDefinition, fieldDefinition.length() - whiteSpaceIndex).trim();

		return new String[]{annotations, fieldData};
	}

	private static String getFieldName(String fieldDefinition)
	{
		fieldDefinition = fieldDefinition.trim();

		int whiteSpaceIndex = StringUtils.indexOf(fieldDefinition, " ");
		return StringUtils.left(fieldDefinition, whiteSpaceIndex);
	}

	public static String getFieldDataType(String attributeDef)
	{
		String dataType = DatabaseCommon.getDataType(attributeDef);
		if(dataType.equals(""))
		{
			throw_Wrong_Format_Exception(attributeDef);
		}
		return dataType;
	}

	public static boolean isPrimaryKey(String attributeDef)
	{
		return attributeDef.toUpperCase().contains("PRIMARY KEY");
	}

	public static boolean isAutoIncremental(String attributeDef)
	{
		return attributeDef.toUpperCase().contains("AUTO_INCREMENT");
	}

	public static void setDefaultValue(DataField field, String attributeDef)
	{
		if(attributeDef.toUpperCase().contains("DEFAULT"))
		{
			if(attributeDef.toUpperCase().contains("DEFAULT NULL"))
			{
				NullFieldValue nullFieldValue = new NullFieldValue(field);
				field.setDefaultFieldValue(nullFieldValue);
			} else
				RuntimeUtils.throwRunTimeException("custom default value capture logic not yet implemented",
						ExitCode.MISSING_IMPLEMENTATION);
		}
	}

	private static void throw_Wrong_Format_Exception(String schemaStr)
	{
		try
		{
			throw new RuntimeException("The attribute defintion " + schemaStr + " is in a wrong format!");
		} catch(RuntimeException e)
		{
			e.printStackTrace();
			System.exit(ExitCode.WRONGCREATTABLEFORMAT);
		}
	}

}
