package com.codefutures.tpcc;

public interface TpccConstants {
    /*
     * correct values
     */
	public static int TRANSACTION_COUNT = 5;
	public static int MAXITEMS = 100000;
	public static int CUST_PER_DIST = 3000;
	public static int DIST_PER_WARE = 10;
	public static int ORD_PER_DIST = 3000;

    public static int[] nums = new int[CUST_PER_DIST];

    /* definitions for new order transaction */
    public static int MAX_NUM_ITEMS = 15;
    public static int MAX_ITEM_LEN = 24;


	// maximum value allowed: 43
	public static final int NEW_ORDER_TXN_RATE = 100;

	// the sum of the next for entries must be less then 57
	public static final int PAYMENT_TXN_RATE = 0;
	public static final int DELIVERY_TXN_RATE = 0;
	public static final int ORDER_STAT_TXN_RATE = 0;
	public static final int SLEV_TXN_RATE = 0;



}
