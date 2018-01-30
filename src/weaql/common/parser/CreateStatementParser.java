/*
 * This class defines methods to parse a create table statement, for example
 * it will return the table name, and return attributes list.
 */

package weaql.common.parser;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import weaql.common.database.constraints.*;
import weaql.common.database.constraints.check.*;
import weaql.common.database.constraints.fk.ForeignKeyConstraint;
import weaql.common.database.constraints.fk.ForeignKeyPolicy;
import weaql.common.database.util.*;
import weaql.common.database.constraints.fk.ForeignKeyAction;
import weaql.common.database.constraints.unique.UniqueConstraint;
import weaql.common.database.field.CRDTFieldType;
import weaql.common.database.field.DataField;
import weaql.common.database.table.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.RuntimeUtils;
import weaql.common.util.ExitCode;


/**
 * The Class CreateStatementParser. Modified by Subhajit on 8/09/17
 */
public class CreateStatementParser
{

	static final Logger LOG = LoggerFactory.getLogger(CreateStatementParser.class);
        private static Map<ForeignKeyConstraint,ExecutionPolicy> foreignKeyPolicies;
	/**
	 * Checks if is _ create_ table_ statement.
	 *
	 * @param schemaStr
	 * 		the schema str
	 *
	 * @return true, if is _ create_ table_ statement
	 */
	public static boolean is_Create_Table_Statement(String schemaStr)
	{
		return schemaStr.toLowerCase().contains("create table");
	}

	/**
	 * Create_ table_ instance.
	 *
	 * @param schemaStr
	 * 		the schema str
	 *
	 * @return the database table
	 */
	public static DatabaseTable createTable(DatabaseMetadata metadata, String schemaStr)
	{
		if(!is_Create_Table_Statement(schemaStr))
			return null;
		else
		{
			String tableTitleStr = get_Table_Title_String(schemaStr);
			String bodyStr = get_Table_Body_String(schemaStr);
			CRDTTableType tableType = get_Table_Type(tableTitleStr);
			ExecutionPolicy tableExecutionPolicy = getTableExecutionPolicy(tableTitleStr);
			String tableName = get_Table_Name(tableTitleStr);
                        //create base fields here
			CreateStatementParser.foreignKeyPolicies = new HashMap<>();
                        LinkedHashMap<String, DataField> fieldsMap = createFields(metadata, tableName, bodyStr);
                        //DatabaseTable dbTable = new DatabaseTable(tableName, tableType, fieldsMap, tableExecutionPolicy);
                        
                     
			DatabaseTable dbTable = new DatabaseTable(tableName, tableType, fieldsMap, tableExecutionPolicy,CreateStatementParser.foreignKeyPolicies);
                        
			for(DataField f : dbTable.getFieldsMap().values())
				f.setDatabaseTable(dbTable);
                        return dbTable;
		}
	}

	/**
	 * Gets the _ table_ title_ string.
	 *
	 * @param schemaStr
	 * 		the schema str
	 *
	 * @return the _ table_ title_ string
	 */
	public static String get_Table_Title_String(String schemaStr)
	{
		int endIndex = schemaStr.indexOf("(");
		return schemaStr.substring(0, endIndex).replaceAll("'", "");
	}

	/**
	 * Gets the _ table_ body_ string.
	 *
	 * @param schemaStr
	 * 		the schema str
	 *
	 * @return the _ table_ body_ string
	 */
	public static String get_Table_Body_String(String schemaStr)
	{
		int startIndex = schemaStr.indexOf("(");
		int endIndex = schemaStr.lastIndexOf(")");

		if(startIndex == -1 || endIndex == -1 || startIndex >= endIndex)
		{
			throw_Wrong_Format_Exception(schemaStr);
		}

		return schemaStr.substring(startIndex + 1, endIndex);
	}

	/**
	 * Gets the _ table_ type_ annotation.
	 *
	 * @param titleStr
	 * 		the title str
	 *
	 * @return the _ table_ type_ annotation
	 */
	private static String get_Table_Type_Annotation(String titleStr)
	{
		int startIndex = titleStr.indexOf("@");
		if(startIndex == -1)
			return ""; // there is no annotation
		int endIndex = titleStr.indexOf(" ", startIndex);
		return titleStr.substring(startIndex + 1, endIndex);
	}

	private static ExecutionPolicy getForeignKeyExecutionPolicy(String titleStr)
	{
		int startIndex = titleStr.indexOf("@",3);

		// default value is UPDATE_WINS
		if(startIndex == -1)
			return ExecutionPolicy.FR;
                        //return ExecutionPolicy.UPDATEWINS;

		int endIndex = titleStr.indexOf(" ", startIndex);
		String annotationStr = titleStr.substring(startIndex + 1, endIndex);
		return ExecutionPolicy.valueOf(annotationStr);
	}

	/**
	 * Gets the _ table_ type.
	 *
	 * @param titleStr
	 * 		the title str
	 *
	 * @return the _ table_ type
	 */
	public static CRDTTableType get_Table_Type(String titleStr)
	{

		String annotStr = get_Table_Type_Annotation(titleStr);
		if(annotStr.equals(""))
			return CRDTTableType.NONCRDTTABLE;
		else
			return CRDTTableType.valueOf(annotStr);

	}

	public static ExecutionPolicy getTableExecutionPolicy(String titleStr)
	{
		String annotation;
		//int startIndex = StringUtils.ordinalIndexOf(titleStr, "@", 2);
                int startIndex = StringUtils.ordinalIndexOf(titleStr, "@", 2);
		if(startIndex == -1)
                        return ExecutionPolicy.AW;
			//return ExecutionPolicy.UPDATEWINS;
		int endIndex = titleStr.indexOf(" ", startIndex);
		annotation = titleStr.substring(startIndex + 1, endIndex);
                //System.out.println("*** titleStr:="+titleStr+" startIndex:="+startIndex);
		return ExecutionPolicy.valueOf(annotation);
	}

	/**
	 * Gets the _ table_ name.
	 *
	 * @param titleStr
	 * 		the title str
	 *
	 * @return the _ table_ name
	 */
	public static String get_Table_Name(String titleStr)
	{
		if(!titleStr.toLowerCase().contains("table"))
		{
			throw_Wrong_Format_Exception(titleStr);
		}
		titleStr = titleStr.replaceAll("\\s+", " ");
		titleStr = titleStr.replaceAll("`", "");
		String[] subStrs = titleStr.split("\\s"); // \\s is space
		if(subStrs.length == 0)
		{
			throw_Wrong_Format_Exception(titleStr);
		}
		return subStrs[subStrs.length - 1];
	}

	/**
	 * Checks if is right comma to split.
	 *
	 * @param str
	 * 		the str
	 * @param beginIndex
	 * 		the begin index
	 * @param commaIndex
	 * 		the comma index
	 *
	 * @return true, if is right comma to split
	 */
	private static boolean isRightCommaToSplit(String str, int beginIndex, int commaIndex)
	{
		int cursorIndex = commaIndex;
		int leftBracket = 0;
		int rightBracket = 0;
		while(cursorIndex >= beginIndex)
		{
			if(str.charAt(cursorIndex) == '(')
			{
				leftBracket++;
			} else if(str.charAt(cursorIndex) == ')')
			{
				rightBracket++;
			}
			cursorIndex--;
		}
		return leftBracket == rightBracket;
	}

	/**
	 * Gets the _ declarations.
	 *
	 * @param bodyStr
	 * 		the body str
	 *
	 * @return the _ declarations
	 */
	public static String[] getDeclarationsStrs(String bodyStr)
	{
		bodyStr = bodyStr.replaceAll("\\s+", " ");
		List<String> declarationList = new ArrayList<>();
		int beginIndex = 0;
		int declarationBeginIndex = 0;
		while(beginIndex < bodyStr.length())
		{
			int commaIndex = bodyStr.indexOf(',', beginIndex);
			if(commaIndex == -1)
			{
				declarationList.add(bodyStr.substring(declarationBeginIndex));
				break;
			} else
			{
				if(isRightCommaToSplit(bodyStr, declarationBeginIndex, commaIndex))
				{
					declarationList.add(bodyStr.substring(declarationBeginIndex, commaIndex));
					beginIndex = commaIndex + 1;
					declarationBeginIndex = beginIndex;
				} else
				{
					//declarationBeginIndex = beginIndex;
					beginIndex = commaIndex + 1;
					continue;
				}
			}
		}
		//search for the comma
		//String[] subStrs = bodyStr.split(",");
		if(declarationList.size() == 0)
		{
			throw_Wrong_Format_Exception(bodyStr);
		}

		String[] subStrs = new String[declarationList.size()];
		for(int i = 0; i < subStrs.length; i++)
		{
			subStrs[i] = declarationList.get(i).trim();
		}
		return subStrs;
	}

	/**
	 * Gets the _ attribute strs.
	 *
	 * @param declarationStrs
	 * 		the declaration strs
	 *
	 * @return the _ attribute strs
	 */
	public static Vector<String> getAttributesStrs(String[] declarationStrs)
	{
		Vector<String> attrStrs = new Vector<>();
		for(int i = 0; i < declarationStrs.length; i++)
		{
			if(!(declarationStrs[i].toUpperCase().startsWith(
					"CONSTRAINT") || declarationStrs[i].toUpperCase().startsWith(
					"PRIMARY KEY") || declarationStrs[i].toUpperCase().startsWith(
					"INDEX") || declarationStrs[i].toUpperCase().startsWith(
					"KEY") || declarationStrs[i].toUpperCase().startsWith(
					"UNIQUE") || declarationStrs[i].toUpperCase().contains(
					"FOREIGN KEY") || declarationStrs[i].toUpperCase().startsWith("CHECK")))
			{
				attrStrs.add(declarationStrs[i]);
			}
		}
		if(attrStrs.size() == 0)
		{
			throw_Wrong_Format_Exception("");
		}
		return attrStrs;
	}

	/**
	 * Gets the _ constraint strs.
	 *
	 * @param declarationStrs
	 * 		the declaration strs
	 *
	 * @return the _ constraint strs
	 */
	public static Vector<String> getConstraintStrs(String[] declarationStrs)
	{
		Vector<String> constraintStrs = new Vector<>();
		for(int i = 0; i < declarationStrs.length; i++)
		{
			if(declarationStrs[i].toUpperCase().startsWith("PRIMARY KEY") || declarationStrs[i].toUpperCase().contains(
					"FOREIGN KEY") || declarationStrs[i].toUpperCase().contains(
					"CHECK") || declarationStrs[i].toUpperCase().contains("UNIQUE"))

				constraintStrs.add(declarationStrs[i]);

		}
		return constraintStrs;
	}

	/**
	 * Gets the _ data_ fields.
	 *
	 * @param tableName
	 * 		the table name
	 * @param attributeStrs
	 * 		the attribute strs
	 *
	 * @return the _ data_ fields
	 */
	public static LinkedHashMap<String, DataField> createTableFields(String tableName, Vector<String> attributeStrs)
	{
		LinkedHashMap<String, DataField> fieldsMap = new LinkedHashMap<>();

		for(int i = 0; i < attributeStrs.size(); i++)
		{
			DataField field = DataFieldParser.createField(tableName, attributeStrs.elementAt(i), i);

			if(fieldsMap.containsKey(field.getFieldName()))
			{
				LOG.error("field {} is duplicated", field.getFieldName());
				RuntimeUtils.throwRunTimeException("duplicated field", ExitCode.DUPLICATED_FIELD);
			}

			fieldsMap.put(field.getFieldName(), field);
		}

		return fieldsMap;
	}

	/**
	 * Update_ data_ fields.
	 *
	 * @param fieldsMap
	 * 		the d fs
	 * @param constraintStrs
	 * 		the constraint strs
	 */
	public static void setFieldsConstraints(DatabaseMetadata metadata, LinkedHashMap<String, DataField> fieldsMap,
											Vector<String> constraintStrs)
	{
		for(int i = 0; i < constraintStrs.size(); i++)
		{
			String constraint = constraintStrs.elementAt(i);

			if(constraint.toUpperCase().contains("PRIMARY KEY") || constraint.toUpperCase().contains("UNIQUE"))
			{
				boolean isPrimaryKey = constraint.toUpperCase().contains("PRIMARY KEY");
                                //System.out.println("***In CreateStatementParser Value of field isPrimaryKey:="+isPrimaryKey);
				int startIndex = constraint.indexOf("(");
				int endIndex = constraint.indexOf(")");
				if(startIndex >= endIndex || startIndex == -1 || endIndex == -1)
					throw_Wrong_Format_Exception(constraint);

				String keyStr = constraint.substring(startIndex + 1, endIndex);
				keyStr = keyStr.replaceAll("\\s", "");
				keyStr = keyStr.replaceAll("`", "");
				String[] pKeys = keyStr.split(",");
                                
                                //System.out.println("***In CreateStatementParser pKeys length:="+pKeys.length);

				boolean requiresCoordination = true;
				boolean containsAutoIncrement = false;

				for(int k = 0; k < pKeys.length; k++)
				{
					if(!fieldsMap.containsKey(pKeys[k]))
						throw_Wrong_Format_Exception(constraint + " " + pKeys[k]);

					DataField field = fieldsMap.get(pKeys[k]);

					if(isPrimaryKey)
						field.setPrimaryKey();
                                        
                                        //System.out.println("***In CreateStatementParser after field.setPrimaryKey()"+field.isPrimaryKey());

					if(field.isAutoIncrement())
						containsAutoIncrement = true;

					// if at least one of the tuples present in the unique key have no semantic, then we can generate a
					// unique value for that column, meaning that the unique constraint will hold at any time
					if(field.getSemantic() == SemanticPolicy.NOSEMANTIC)
						requiresCoordination = false;
				}

				if(containsAutoIncrement)
					requiresCoordination = false;

				Constraint uniqueConstraint = new UniqueConstraint(requiresCoordination, isPrimaryKey);

				for(int j = 0; j < pKeys.length; j++)
				{
					if(!fieldsMap.containsKey(pKeys[j]))
						throw_Wrong_Format_Exception(constraint + " " + pKeys[j]);

					DataField field = fieldsMap.get(pKeys[j]);
					uniqueConstraint.addField(field);
					uniqueConstraint.setTableName(field.getTableName());
					field.addConstraint(uniqueConstraint);
				}

				uniqueConstraint.generateIdentifier();
			} else if(constraint.toUpperCase().contains("FOREIGN KEY"))
			{
				ExecutionPolicy executionPolicy = ExecutionPolicy.FR;

				//@info: here use the default behaviour of MySQL

				int locationIndex = constraint.toUpperCase().indexOf("FOREIGN KEY");
				int startIndex = constraint.indexOf("(", locationIndex);
				int endIndex = constraint.indexOf(")", locationIndex);

				if(startIndex >= endIndex || startIndex == -1 || endIndex == -1)
					throw_Wrong_Format_Exception(constraint);

				String keyStr = constraint.substring(startIndex + 1, endIndex);
				keyStr = keyStr.replaceAll("\\s", "");
				keyStr = keyStr.replaceAll("`", "");
				String[] fKeys = keyStr.split(",");

                                int locationIndex_FKRule = StringUtils.ordinalIndexOf(constraint, "@", 1);
                                //if(startIndex == -1)
                                        executionPolicy = ExecutionPolicy.FR;
                                int locationIndex_2 = constraint.toUpperCase().indexOf("REFERENCES");
                                
                                //String annotationFKRule = constraint.substring(locationIndex_FKRule+1, locationIndex_2);
                                //executionPolicy = ExecutionPolicy.valueOf(annotationFKRule);*/
                                
                                
                                ForeignKeyAction updatePolicy = ForeignKeyAction.RESTRICT;
				ForeignKeyAction deletePolicy = ForeignKeyAction.RESTRICT;
                                
				if(constraint.toUpperCase().contains("ON UPDATE CASCADE"))
					updatePolicy = ForeignKeyAction.CASCADE;
				if(constraint.toUpperCase().contains("ON UPDATE SET NULL"))
					updatePolicy = ForeignKeyAction.SET_NULL;
				if(constraint.toUpperCase().contains("ON UPDATE RESTRICT"))
					updatePolicy = ForeignKeyAction.RESTRICT;

				if(constraint.toUpperCase().contains("ON DELETE CASCADE"))
					deletePolicy = ForeignKeyAction.CASCADE;
				if(constraint.toUpperCase().contains("ON DELETE SET NULL"))
					deletePolicy = ForeignKeyAction.SET_NULL;
				if(constraint.toUpperCase().contains("ON DELETE RESTRICT"))
					deletePolicy = ForeignKeyAction.RESTRICT;
                                
                                ForeignKeyPolicy fkPolicy = new ForeignKeyPolicy(updatePolicy, deletePolicy, executionPolicy);

				ForeignKeyConstraint fkConstraint = new ForeignKeyConstraint(fkPolicy);
                                
				int startIndex_2 = constraint.indexOf(" ", locationIndex_2);
				int endIndex_2 = constraint.indexOf("(", startIndex_2);
				int endIndex_3 = constraint.indexOf(")", endIndex_2);

				String foreignKeyStr = constraint.substring(endIndex_2 + 1, endIndex_3);
				foreignKeyStr = foreignKeyStr.replaceAll("\\s", "");
				foreignKeyStr = foreignKeyStr.replaceAll("`", "");
				String[] foreignAttributes = foreignKeyStr.split(",");

				if(foreignAttributes.length != fKeys.length)
					RuntimeUtils.throwRunTimeException("foreign attributes size do not match",
							ExitCode.WRONGCREATTABLEFORMAT);

				String foreignKeyTable = constraint.substring(startIndex_2 + 1, endIndex_2).trim();

				for(int t = 0; t < fKeys.length; t++)
				{
					if(!fieldsMap.containsKey(fKeys[t]))
						RuntimeUtils.throwRunTimeException("foreign attributes size do not match",
								ExitCode.WRONGCREATTABLEFORMAT);

					DataField childField = fieldsMap.get(fKeys[t]);
					DataField parentField = metadata.getTable(foreignKeyTable).getField(foreignAttributes[t]);

					parentField.getTable().addChildTableConstraint(fkConstraint);
					fkConstraint.addParentChildRelation(parentField, childField);
					fkConstraint.setParentTable(parentField.getTable());
					fkConstraint.setTableName(childField.getTableName());
					childField.setForeignKey();
					childField.addConstraint(fkConstraint);
                                        
                                        CreateStatementParser.foreignKeyPolicies.put(fkConstraint, executionPolicy);
				}

				fkConstraint.generateIdentifier();
			} else if(constraint.toUpperCase().contains("CHECK"))
			{
				int locationIndex = constraintStrs.elementAt(i).toUpperCase().indexOf("CHECK");
				int startIndex = constraintStrs.elementAt(i).indexOf("(", locationIndex);
				int endIndex = constraintStrs.elementAt(i).indexOf(")", locationIndex);
				if(startIndex >= endIndex || startIndex == -1 || endIndex == -1)
				{
					throw_Wrong_Format_Exception(constraintStrs.elementAt(i));
				}

				String conditionStr = constraintStrs.elementAt(i).substring(startIndex + 1, endIndex);

				if(conditionStr.contains("<="))
				{
					String operands[] = conditionStr.split("<=");
					DataField field = fieldsMap.get(operands[0]);
					Constraint checkConstraint;

					operands[1] = operands[1].replaceAll("`", "").replaceAll("\"", "");

					if(field.getCrdtType() == CRDTFieldType.NORMALFLOAT || field.getCrdtType() ==
							CRDTFieldType.LWWFLOAT | field.getCrdtType() == CRDTFieldType.NUMDELTAFLOAT)
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 3, true);
					else if(field.getCrdtType() == CRDTFieldType.NORMALINTEGER || field.getCrdtType() ==
							CRDTFieldType.LWWINTEGER | field.getCrdtType() == CRDTFieldType.NORMALINTEGER)
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 2, true);
					else if(field.getCrdtType() == CRDTFieldType.NORMALDOUBLE || field.getCrdtType() ==
							CRDTFieldType.LWWDOUBLE | field.getCrdtType() == CRDTFieldType.NUMDELTADOUBLE)
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 4, true);
					else
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 1, true);

					checkConstraint.addField(field);
					checkConstraint.setTableName(field.getTableName());
					checkConstraint.generateIdentifier();
					field.addConstraint(checkConstraint);

				} else if(conditionStr.contains("<"))
				{
					String operands[] = conditionStr.split("<");
					DataField field = fieldsMap.get(operands[0]);

					operands[1] = operands[1].replaceAll("`", "").replaceAll("\"", "");

					Constraint checkConstraint;

					if(field.getCrdtType() == CRDTFieldType.NORMALFLOAT || field.getCrdtType() ==
							CRDTFieldType.LWWFLOAT | field.getCrdtType() == CRDTFieldType.NUMDELTAFLOAT)
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 3, false);
					else if(field.getCrdtType() == CRDTFieldType.NORMALINTEGER || field.getCrdtType() ==
							CRDTFieldType.LWWINTEGER | field.getCrdtType() == CRDTFieldType.NORMALINTEGER)
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 2, false);
					else if(field.getCrdtType() == CRDTFieldType.NORMALDOUBLE || field.getCrdtType() ==
							CRDTFieldType.LWWDOUBLE | field.getCrdtType() == CRDTFieldType.NUMDELTADOUBLE)
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 4, false);
					else
						checkConstraint = new CheckConstraint(CheckConstraintType.LESSER, operands[1], 1, false);

					checkConstraint.addField(field);
					checkConstraint.setTableName(field.getTableName());
					checkConstraint.generateIdentifier();
					field.addConstraint(checkConstraint);

				} else if(conditionStr.contains(">="))
				{
					String operands[] = conditionStr.split(">=");
					DataField field = fieldsMap.get(operands[0]);
					operands[1] = operands[1].replaceAll("`", "").replaceAll("\"", "");
					Constraint checkConstraint;

					if(field.getCrdtType() == CRDTFieldType.NORMALFLOAT || field.getCrdtType() ==
							CRDTFieldType.LWWFLOAT | field.getCrdtType() == CRDTFieldType.NUMDELTAFLOAT)
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 3, true);
					else if(field.getCrdtType() == CRDTFieldType.NORMALINTEGER || field.getCrdtType() ==
							CRDTFieldType.LWWINTEGER | field.getCrdtType() == CRDTFieldType.NORMALINTEGER)
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 2, true);
					else if(field.getCrdtType() == CRDTFieldType.NORMALDOUBLE || field.getCrdtType() ==
							CRDTFieldType.LWWDOUBLE | field.getCrdtType() == CRDTFieldType.NUMDELTADOUBLE)
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 4, true);
					else
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 1, true);

					checkConstraint.addField(field);
					checkConstraint.setTableName(field.getTableName());
					checkConstraint.generateIdentifier();
					field.addConstraint(checkConstraint);

				} else if(conditionStr.contains(">"))
				{
					String operands[] = conditionStr.split(">");
					DataField field = fieldsMap.get(operands[0]);
					operands[1] = operands[1].replaceAll("`", "").replaceAll("\"", "");
					Constraint checkConstraint;

					if(field.getCrdtType() == CRDTFieldType.NORMALFLOAT || field.getCrdtType() ==
							CRDTFieldType.LWWFLOAT | field.getCrdtType() == CRDTFieldType.NUMDELTAFLOAT)
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 3, false);
					else if(field.getCrdtType() == CRDTFieldType.NORMALINTEGER || field.getCrdtType() ==
							CRDTFieldType.LWWINTEGER | field.getCrdtType() == CRDTFieldType.NUMDELTAINTEGER)
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 2, false);
					else if(field.getCrdtType() == CRDTFieldType.NORMALDOUBLE || field.getCrdtType() ==
							CRDTFieldType.LWWDOUBLE | field.getCrdtType() == CRDTFieldType.NUMDELTADOUBLE)
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 4, false);
					else
						checkConstraint = new CheckConstraint(CheckConstraintType.GREATER, operands[1], 1, false);

					checkConstraint.addField(field);
					checkConstraint.setTableName(field.getTableName());
					checkConstraint.generateIdentifier();
					field.addConstraint(checkConstraint);

				} else
					throw_Wrong_Format_Exception(constraintStrs.elementAt(i));

			} else
			{
				RuntimeUtils.throwRunTimeException("unexpected constraint", ExitCode.UNKNOWN_INVARIANT);
			}
		}
	}

	/**
	 * Gets the _ data_ field_ hash map.
	 *
	 * @param tableName
	 * 		the table name
	 * @param bodyStr
	 * 		the body str
	 *
	 * @return the _ data_ field_ hash map
	 */
	public static LinkedHashMap<String, DataField> createFields(DatabaseMetadata metadata, String tableName,
																String bodyStr)
	{
		String[] declarations = getDeclarationsStrs(bodyStr);
		Vector<String> attrStrs = getAttributesStrs(declarations);
		Vector<String> consStrs = getConstraintStrs(declarations);

		LinkedHashMap<String, DataField> fieldsMap = createTableFields(tableName, attrStrs);
		setFieldsConstraints(metadata, fieldsMap, consStrs);
		return fieldsMap;
	}

	/**
	 * Throw_ wrong_ format_ exception.
	 *
	 * @param schemaStr
	 * 		the schema str
	 */
	private static void throw_Wrong_Format_Exception(String schemaStr)
	{
		try
		{
			throw new RuntimeException("The create table statment " + schemaStr + " is in a wrong format!");
		} catch(RuntimeException e)
		{
			e.printStackTrace();
			System.exit(ExitCode.WRONGCREATTABLEFORMAT);
		}
	}
}
