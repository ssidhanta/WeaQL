package weaql.common.database.constraints.fk;


import weaql.common.database.util.ExecutionPolicy;


/**
 * Created by dnlopes on 08/05/15.
 */
public final class ForeignKeyPolicy
{

	private final ForeignKeyAction updateAction;
	private final ForeignKeyAction deleteAction;
	private final ExecutionPolicy executionPolicy;

	public ForeignKeyPolicy(ForeignKeyAction updateType, ForeignKeyAction deleteType, ExecutionPolicy executionPolicy)
	{
		this.updateAction = updateType;
		this.deleteAction = deleteType;
		this.executionPolicy = executionPolicy;
	}

	public ForeignKeyAction getUpdateAction()
	{
		return updateAction;
	}

	public ForeignKeyAction getDeleteAction()
	{
		return deleteAction;
	}

	public ExecutionPolicy getExecutionPolicy()
	{
		return this.executionPolicy;
	}
}
