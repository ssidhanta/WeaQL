package weaql.common.util.defaults;

/**
 * Created by dnlopes on 10/03/15.
 */
public interface ScratchpadDefaults
{
	int RDBMS_H2 = 1;
	int RDBMS_MYSQL = 2;
	int SQL_ENGINE = RDBMS_MYSQL;

	String SCRATCHPAD_TABLE_ALIAS_PREFIX = "_SPT_";
	String SCRATCHPAD_TEMPTABLE_ALIAS_PREFIX = "_TSPT_";
	String SCRATCHPAD_COL_PREFIX = "_SP_";
	String SCRATCHPAD_COL_TS = "_SP_ts";
	String SCRATCHPAD_IDS_TABLE = SCRATCHPAD_TABLE_ALIAS_PREFIX + "SCRATCHPAD_IDS";
}
