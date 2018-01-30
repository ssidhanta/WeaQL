/*
 * This class defines a set of types of CRDT
 */
package weaql.common.database.field;

/**
 * The Enum CrdtDataFieldType.
 */
public enum CRDTFieldType
{

	// data fields
	/** The noncrdtfield. */
	NONCRDTFIELD, 
	
	/** The normalinteger. */
	NORMALINTEGER, 
	 
	/** The normalfloat. */
	NORMALFLOAT, 
	
	/** The normaldouble. */
	NORMALDOUBLE, 
	 
	/** The normalstring. */
	NORMALSTRING, 
	 
	/** The normaldatetime. */
	NORMALDATETIME, 
	
	/** The normalboolean. */
	NORMALBOOLEAN, 
 
	/** The lwwinteger. */
	LWWINTEGER, 
	 
	/** The lwwfloat. */
	LWWFLOAT, 
	
	/** The lwwdouble. */
	LWWDOUBLE, 
	 
	/** The lwwstring. */
	LWWSTRING, 
	 
	/** The lwwdatetime. */
	LWWDATETIME, 
	 
	/** The lwwboolean. */
	LWWBOOLEAN, 
	 
	/** The lwwdeletedflag. */
	LWWDELETEDFLAG, 
	 
	/** The lwwlogicaltimestamp. */
	LWWLOGICALTIMESTAMP, 
	 
	/** The numdeltainteger. */
	NUMDELTAINTEGER, 
	 
	/** The numdeltafloat. */
	NUMDELTAFLOAT, 
	 
	/** The numdeltadouble. */
	NUMDELTADOUBLE, 
	 
	/** The numdeltadatetime. */
	NUMDELTADATETIME,

	IMMUTABLE_FIELD
	
}
