package weaql.client.execution.temporary.scratchpad;


import weaql.client.operation.SQLWriteOperation;
import weaql.client.execution.temporary.ReadOnlyInterface;
import weaql.client.execution.temporary.WriteSet;
import weaql.common.database.Record;

import java.sql.SQLException;
import java.util.List;


/**
 *
 * Created by dnlopes on 25/09/15.
 * A more complete version of the ReadOnlyScratchpad
 * Besides query statements, it allows for update and insert sql statements
 *
 */
public interface IDBScratchpad extends ReadOnlyInterface
{
	void clearScratchpad() throws SQLException;
	int executeUpdate(SQLWriteOperation op) throws SQLException;
	WriteSet getWriteSet();
	List<Record> getScratchpadSnapshot() throws SQLException;
}
