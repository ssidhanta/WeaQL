package weaql.common.database.table;

/**
 * The Enum CrdtTableType.
 */
public enum CRDTTableType
{
	NONCRDTTABLE,
	READ_ONLY_TABLE, // read-only table
	AOSETTABLE, // insert only table
	UOSETTABLE, // update only table
	AUSETTABLE, // insert and update table
	ARSETTABLE, // insert, delete and update table
        AW,
        RW,

}
