package weaql.client.execution;


import weaql.common.database.constraints.fk.ForeignKeyConstraint;
import weaql.common.database.constraints.fk.ParentChildRelation;
import weaql.common.database.field.DataField;
import weaql.common.database.table.DatabaseTable;
import weaql.common.database.util.Row;
import weaql.common.util.defaults.DatabaseDefaults;
import weaql.common.util.defaults.ScratchpadDefaults;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * @author dnlopes
 *         This class implements helper methods that build handy SQL queries for use in runtime
 */
public class QueryCreator
{

	private static final String NOT_DELETED_EXPRESSION = DatabaseDefaults.DELETED_COLUMN + "=0";

	/**
	 * Generates a SQL query that selects all child rows that are pointing to the given parent row
	 *
	 * @param parentRow
	 * @param table
	 * @param relations
	 *
	 * @return a sql query
	 */
	public static String findChildFromTableQuery(Row parentRow, DatabaseTable table,
												 List<ParentChildRelation> relations)
	{
		StringBuilder buffer = new StringBuilder();

		buffer.append("SELECT ");
		buffer.append(table.getNormalFieldsSelection());
		buffer.append(" FROM ");
		buffer.append(table.getName());
		buffer.append(" WHERE ");

		Iterator<ParentChildRelation> relationsIt = relations.iterator();

		while(relationsIt.hasNext())
		{
			ParentChildRelation relation = relationsIt.next();
			buffer.append(relation.getChild().getFieldName());
			buffer.append("=");
			buffer.append(parentRow.getFieldValue(relation.getParent().getFieldName()).getFormattedValue());

			if(relationsIt.hasNext())
				buffer.append(" AND ");
		}

		return buffer.toString();
	}

	public static String createFindChildQuery(Row parentRow, DatabaseTable table, List<ParentChildRelation> relations)
	{
		StringBuilder buffer = new StringBuilder();

		buffer.append("SELECT ");
		buffer.append(table.getPrimaryKeyString());
		buffer.append(" FROM ");
		buffer.append(table.getName());
		buffer.append(" WHERE ");

		Iterator<ParentChildRelation> relationsIt = relations.iterator();

		while(relationsIt.hasNext())
		{
			ParentChildRelation relation = relationsIt.next();
			buffer.append(relation.getChild().getFieldName());
			buffer.append("=");
			buffer.append(parentRow.getFieldValue(relation.getParent().getFieldName()).getFormattedValue());

			if(relationsIt.hasNext())
				buffer.append(" AND ");
		}

		return buffer.toString();
	}

	/**
	 * Generates a SQL query to find the matching parent row for the given child row, that is associated with the
	 * given foreign key
	 *
	 * @param childRow
	 * @param constraint
	 *
	 * @return a sql query
	 */
	public static String findParent(Row childRow, ForeignKeyConstraint constraint, int sandboxId)
	{
		DatabaseTable remoteTable = constraint.getParentTable();
		String tempTable = ScratchpadDefaults.SCRATCHPAD_TABLE_ALIAS_PREFIX + remoteTable.getName() + "_" + sandboxId;

		StringBuilder buffer = new StringBuilder();
		StringBuilder buffer2 = new StringBuilder();

		buffer.append("SELECT ");
		buffer.append(remoteTable.getPrimaryKeyString());
		buffer.append(" FROM ");
		buffer.append(remoteTable.getName());
		buffer.append(" WHERE ");
		buffer2.append("SELECT ");
		buffer2.append(remoteTable.getPrimaryKeyString());
		buffer2.append(" FROM ");
		buffer2.append(tempTable);
		buffer2.append(" WHERE ");

		Iterator<ParentChildRelation> relationsIt = constraint.getFieldsRelations().iterator();

		while(relationsIt.hasNext())
		{
			ParentChildRelation relation = relationsIt.next();
			DataField childField = relation.getChild();
			DataField parentField = relation.getParent();

			buffer.append(parentField.getFieldName());
			buffer.append("=");
			buffer.append(childRow.getFieldValue(childField.getFieldName()).getFormattedValue());
			buffer2.append(parentField.getFieldName());
			buffer2.append("=");
			buffer2.append(childRow.getFieldValue(childField.getFieldName()).getFormattedValue());

			if(relationsIt.hasNext())
			{
				buffer.append(" AND ");
				buffer2.append(" AND ");
			}
		}

		buffer.append(" UNION ");
		buffer.append(buffer2);

		return buffer.toString();
	}

	/**
	 * Generates a SQL query that SELECTs the given field of the given row
	 *
	 * @param parentRow
	 * @param field
	 *
	 * @return a sql query
	 */
	public static String selectFieldFromRow(Row parentRow, DataField field, boolean filterDeleted)
	{
		StringBuilder buffer = new StringBuilder();

		buffer.append("SELECT ");
		buffer.append(field.getFieldName());
		buffer.append(" FROM ");
		buffer.append(parentRow.getTable().getName());
		buffer.append(" WHERE ");
		buffer.append(parentRow.getPrimaryKeyValue().getPrimaryKeyWhereClause());

		if(filterDeleted)
		{
			buffer.append(" AND ");
			buffer.append(NOT_DELETED_EXPRESSION);
		}

		return buffer.toString();
	}

	/**
	 * Count the number of visible parents.
	 * @param fkConstraints
	 * @return
	 */
	public static String countParentsVisible(Map<ForeignKeyConstraint, Row> fkConstraints)
	{
		StringBuilder buffer = new StringBuilder();
		buffer.append("SELECT SUM(cnt) FROM (");

		Iterator<ForeignKeyConstraint> fkConstraintsIterator = fkConstraints.keySet().iterator();

		while(fkConstraintsIterator.hasNext())
		{
			String countQuery = countRowsInSelectStatement(fkConstraints.get(fkConstraintsIterator.next()));
			buffer.append(countQuery);

			if(fkConstraintsIterator.hasNext())
				buffer.append(" UNION ALL ");
		}

		buffer.append(") as x");

		return buffer.toString();
	}

	private static String countRowsInSelectStatement(Row row)
	{
		StringBuilder buffer = new StringBuilder();

		buffer.append("SELECT COUNT(*) as cnt FROM ");
		buffer.append(row.getTable().getName());
		buffer.append(" WHERE ");
		buffer.append(row.getPrimaryKeyValue().getPrimaryKeyWhereClause());
		buffer.append(" AND ");
		buffer.append(NOT_DELETED_EXPRESSION);

		return buffer.toString();
	}
}
