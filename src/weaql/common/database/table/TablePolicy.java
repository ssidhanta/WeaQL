package weaql.common.database.table;


/**
 * Created by dnlopes on 23/10/15.
 */
public class TablePolicy
{

	private final CRDTTableType type;
	private final boolean allowInserts;
	private final boolean allowUpdates;
	private final boolean allowDeletes;

	public TablePolicy(CRDTTableType type)
	{
		this.type = type;

		if(this.type == CRDTTableType.ARSETTABLE || this.type == CRDTTableType.AUSETTABLE || this.type ==
				CRDTTableType.AOSETTABLE)
			this.allowInserts = true;
		else
			this.allowInserts = false;

		if(this.type == CRDTTableType.ARSETTABLE)
			this.allowDeletes = true;
		else
			this.allowDeletes = false;

		if(this.type == CRDTTableType.UOSETTABLE || this.type == CRDTTableType.AUSETTABLE || this.type ==
				CRDTTableType.ARSETTABLE)
			this.allowUpdates = true;
		else
			this.allowUpdates = false;
	}

	public boolean allowInserts()
	{
		return this.allowInserts;
	}

	public boolean allowUpdates()
	{
		return this.allowUpdates;
	}

	public boolean allowDeletes()
	{
		return this.allowDeletes;
	}

}
