/*
 * This class defines all possible runtime exception types
 */

package weaql.common.util;


/**
 * The Class RuntimeExceptionType.
 */
public final class ExitCode
{

	/** The Constant NOINITIALIZATION. */
	public static final int NOINITIALIZATION = 0;

	/** The Constant HASHMAPDUPLICATE. */
	public static final int HASHMAPDUPLICATE = 1;

	/** The Constant HASHMAPNOEXIST. */
	public static final int HASHMAPNOEXIST = 2;

	/** The Constant SQLSELECTIONFAIL. */
	public static final int SQLSELECTIONFAIL = 3;

	/** The Constant NULLPOINTER. */
	public static final int NULLPOINTER = 4;

	/** The Constant SQLRESULTSETEMPTY. */
	public static final int SQLRESULTSETEMPTY = 5;

	/** The Constant SQLRESULTSETNOTFOUND. */
	public static final int SQLRESULTSETNOTFOUND = 6;

	/** The Constant INVALIDUSAGE. */
	public static final int INVALIDUSAGE = 7;

	/** The Constant SCHEMANOCREATSTAT. */
	public static final int SCHEMANOCREATSTAT = 8;

	/** The Constant SCHEMANOCRDTTABLE. */
	public static final int SCHEMANOCRDTTABLE = 9;

	/** The Constant NOTDEFINEDCRDTTABLE. */
	public static final int NOTDEFINEDCRDTTABLE = 10;

	/** The Constant WRONGCREATTABLEFORMAT. */
	public static final int WRONGCREATTABLEFORMAT = 11;

	/** The Constant UNKNOWNTABLEANNOTYPE. */
	public static final int UNKNOWNTABLEANNOTYPE = 12;

	/** The Constant UNKNOWNDATAFIELDANNOTYPE. */
	public static final int UNKNOWNDATAFIELDANNOTYPE = 13;

	/** The Constant READONLYTBLWRONGANNO. */
	public static final int READONLYTBLWRONGANNO = 14;

	/** The Constant NOTDELTEQUERY. */
	public static final int NOTDELTEQUERY = 15;

	/** The Constant NOTINSETQUERY. */
	public static final int NOTINSETQUERY = 16;

	/** The Constant NOTUPDATEQUERY. */
	public static final int NOTUPDATEQUERY = 17;

	/** The Constant EMPTYVALUENODEFAULT. */
	public static final int EMPTYVALUENODEFAULT = 18;

	/** The Constant UNKNOWNLWWDATATYPE. */
	public static final int UNKNOWNLWWDATATYPE = 19;

	/** The Constant UNKNOWNNONLWWDATATYPE. */
	public static final int UNKNOWNNONLWWDATATYPE = 20;

	/** The Constant UNKNOWSQLQUERY. */
	public static final int UNKNOWSQLQUERY = 21;

	/** The Constant UNKNOWTABLENAME. */
	public static final int UNKNOWTABLENAME = 22;

	/** The Constant NONPRIMARYKEY. */
	public static final int NONPRIMARYKEY = 23;

	/** The Constant FILENOTFOUND. */
	public static final int FILENOTFOUND = 24;

	/** The Constant ERRORTRANSFORM. */
	public static final int ERRORTRANSFORM = 25;

	/** The Constant NORESULT. */
	public static final int NORESULT = 26;

	/** The Constant OUTOFRANGE. */
	public static final int OUTOFRANGE = 27;

	/** The Constant FOREIGNPRIMARYKEYMISSING. */
	public static final int FOREIGNPRIMARYKEYMISSING = 28;

	/** The Constant ERRORNOTNULL. */
	public static final int ERRORNOTNULL = 29;

	public static final int SCRATCHPAD_INIT_FAILED = 30;

	public static final int MISSING_IMPLEMENTATION = 31;

	public static final int UNEXPECTED_TABLE = 32;

	public static final int EXECUTOR_NOT_FOUND = 33;
	public static final int AUTO_COMMIT_NOT_SUPPORTED = 33;
	public static final int UNKNOWN_INVARIANT = 36;
	public static final int MULTI_TABLE_UPDATE = 36;
	public static final int UNEXPECTED_OP = 37;
	public static final int XML_ERROR = 38;
	public static final int WRONG_ARGUMENTS_NUMBER = 39;
	public static final int DUPLICATED_FIELD = 39;
	public static final int FETCH_RESULTS_ERROR = 40;
	public static final int ID_GENERATOR_ERROR = 41;

	public static final int SANDBOX_INIT_FAILED = 45;

	public static final int CLASS_NOT_FOUND = 46;

}