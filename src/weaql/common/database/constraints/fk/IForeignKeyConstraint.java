package weaql.common.database.constraints.fk;


import weaql.common.database.constraints.Constraint;
import weaql.common.database.table.DatabaseTable;


/**
 * Created by dnlopes on 24/03/15.
 */
public interface IForeignKeyConstraint extends Constraint
{

	public void setParentTable(DatabaseTable table);
	public void setChildTable(DatabaseTable childTable);

	public DatabaseTable getParentTable();
}
