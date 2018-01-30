package applications.tpcc;


/**
 * Created by dnlopes on 05/09/15.
 */
public class TpccConstants
{

	public static final int BUCKETS_NUMBER = 30;
	public static final int LATENCY_INTERVAL = 10;
	// transactions probabilities
	// sum must be 100
	public static int NEW_ORDER_TXN_RATE = 100;		//45 is default
	public static int PAYMENT_TXN_RATE = 0;		//43 is default
	public static int DELIVERY_TXN_RATE = 0;	//4 is default

	public static int ORDER_STAT_TXN_RATE = 0;	//4 is default ; read only
	public static int STOCK_LEVEL_TXN_RATE = 0;	//4 is default ; read only

	// general constants
	public static int WAREHOUSES_NUMBER = 10;
	public static int DISTRICTS_PER_WAREHOUSE = 10;
	public static int CUSTOMER_PER_DISTRICT = 3000;
	public static int MAXITEMS = 100000;
	public static boolean ALLOW_MULTI_WAREHOUSE_TX = true;

	// constants for NewOrder
	public static int MAX_NUM_ITEMS = 15;
	//public static int MAX_ITEM_LEN = 24;


	public static String NEW_ORDER_TXN_NAME = "neworder";
	public static String PAYMENT_TXN_NAME = "payment";
	public static String DELIVERY_TXN_NAME = "delivery";
	public static String ORDER_STAT_TXN_NAME = "orderstat";
	public static String STOCK_LEVEL_TXN_NAME = "stock";

	public static String[] TXNS_NAMES = {NEW_ORDER_TXN_NAME, PAYMENT_TXN_NAME, DELIVERY_TXN_NAME, ORDER_STAT_TXN_NAME,
			STOCK_LEVEL_TXN_NAME};
}
