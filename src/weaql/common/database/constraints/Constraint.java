package weaql.common.database.constraints;


import weaql.common.database.field.DataField;

import java.util.List;


/**
 * Created by dnlopes on 24/03/15.
 */
public interface Constraint
{

	List<DataField> getFields();

	ConstraintType getType();

	void addField(DataField field);

	String getConstraintIdentifier();

	void generateIdentifier();

	void setTableName(String name);

	String getTableName();

	boolean constainsField(DataField field);

	boolean requiresCoordination();
	void setRequiresCoordination(boolean requiresCoordination);

}
