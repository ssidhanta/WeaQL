package com.codefutures.tpcc;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.*;

import com.codefutures.tpcc.stats.PerformanceCounters;
import com.codefutures.tpcc.stats.ThreadStatistics;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class Driver implements TpccConstants
{

	private static final Logger logger = LoggerFactory.getLogger(Driver.class);
	private static final boolean DEBUG = logger.isDebugEnabled();
	private PerformanceCounters performanceCounter;

	public PerformanceCounters getPerformanceCounter()
	{
		return performanceCounter;
	}

	/**
	 * Can be disabled for debug use only.
	 */
	private static final boolean ALLOW_MULTI_WAREHOUSE_TX = true;

	//CHECK: The following variables are externs??
	//    public int counting_on;
	public int num_ware;
	public int num_conn;

	public int num_node;
	//    public int time_count;
	//    public PrintWriter freport_file;

	// total count for all threads
	private int[] success;
	private int[] late;
	private int[] retry;
	private int[] failure;

	// per thread counts
	private int[][] success2;
	private int[][] late2;
	private int[][] retry2;
	private int[][] failure2;
	private double[] latencies;

	public double[] max_rt = new double[TRANSACTION_COUNT];

	//Private variables
	private final int MAX_RETRY = 1;
	private final int RTIME_NEWORD = 5 * 1000;
	private final int RTIME_PAYMENT = 5 * 1000;
	private final int RTIME_ORDSTAT = 5 * 1000;
	private final int RTIME_DELIVERY = 5 * 1000;
	private final int RTIME_SLEV = 20 * 1000;

	private Connection conn;
	private TpccStatements pStmts;

	// Transaction instances.
	private NewOrder newOrder;
	private Payment payment;
	private OrderStat orderStat;
	private Slev slev;
	private Delivery delivery;

	private final ThreadStatistics stats;

	/**
	 * Constructor.
	 *
	 * @param conn
	 */
	public Driver(ThreadStatistics stats, Connection conn, int fetchSize, int[] success, int[] late, int[] retry,
				  int[] failure, int[][] success2, int[][] late2, int[][] retry2, int[][] failure2, double[] latencies,
				  boolean joins)
	{
		this.performanceCounter = new PerformanceCounters();
		this.stats = stats;
		this.conn = conn;

		try
		{
			pStmts = new TpccStatements(conn, fetchSize);
		} catch(SQLException e)
		{
			logger.error("error initializing TpccStatements for client driver", e);
			System.exit(1);
		}

		// Initialize the transactions.
		newOrder = new NewOrder(pStmts, joins);
		payment = new Payment(pStmts);
		orderStat = new OrderStat(pStmts);
		slev = new Slev(pStmts);
		delivery = new Delivery(pStmts);

		this.success = success;
		this.late = late;
		this.retry = retry;
		this.failure = failure;

		this.success2 = success2;
		this.late2 = late2;
		this.retry2 = retry2;
		this.failure2 = failure2;
		this.latencies = latencies;

		for(int i = 0; i < TRANSACTION_COUNT; i++)
		{
			max_rt[i] = 0.0;
		}
	}

	private final Executor exec = Executors.newSingleThreadExecutor();

	public int runBenchmark(final int t_num, final int numWare, final int numConn)
	{

		num_ware = numWare;
		num_conn = numConn;

		int count = 0;

        /* Actually, WaitTimes are needed... */
		//CHECK: Is activate_transaction handled correctly?
		int sequence = Util.seqGet();

		while(Tpcc.activate_transaction == 1)
		{
			if(DEBUG)
				logger.debug("BEFORE runTransaction: sequence: " + sequence);

			doNextTransaction(t_num, sequence);
			count++;

			if(DEBUG)
				logger.debug("AFTER runTransaction: sequence: " + sequence);
			sequence = Util.seqGet();
		}

		logger.info("Driver terminated after {} transactions", count);
		return 0;
	}

	private void doNextTransaction(int t_num, int sequence)
	{
		int success = 0;

		/*
		int random = Util.randomNumber(0,100);

		if(random < TpccConstants.DELIVERY_TXN_RATE)
			success = doDelivery(t_num);
		else if(random < TpccConstants.DELIVERY_TXN_RATE + TpccConstants.NEW_ORDER_TXN_RATE)
			success = doNeword(t_num);
		else if(random < TpccConstants.DELIVERY_TXN_RATE + TpccConstants.NEW_ORDER_TXN_RATE + TpccConstants.ORDER_STAT_TXN_RATE)
			success = doOrdstat(t_num);
		else if(random < TpccConstants.DELIVERY_TXN_RATE + TpccConstants.NEW_ORDER_TXN_RATE + TpccConstants
				.ORDER_STAT_TXN_RATE + TpccConstants.PAYMENT_TXN_RATE)
			success = doPayment(t_num);
		else
			success = doSlev(t_num);      */


		if(sequence == 0)
		{
			success = doNeword(t_num);
		} else if(sequence == 1)
		{
			success = doPayment(t_num);
		} else if(sequence == 2)
		{
			success = doOrdstat(t_num);
		} else if(sequence == 3)
		{
			success = doDelivery(t_num);
		} else if(sequence == 4)
		{
			success = doSlev(t_num);
		} else
		{
			logger.error("unkown sequence number: {}", sequence);
			System.exit(1);
		}

		/*
		int success = 0;
		if(sequence == 0)
		{
			success = doNeword(t_num);
		} else if(sequence == 1)
		{
			success = doPayment(t_num);
		} else if(sequence == 2)
		{
			success = doOrdstat(t_num);
		} else if(sequence == 3)
		{
			success = doDelivery(t_num);
		} else if(sequence == 4)
		{
			success = doSlev(t_num);
		} else
		{
			logger.error("unkown sequence number: {}", sequence);
			System.exit(1);
		}
		*/

	}

	/*
	  * prepare data and execute the new order transaction for one order
	  * officially, this is supposed to be simulated terminal I/O
	  */
	private int doNeword(int t_num)
	{
		int c_num, i, ret, w_id, d_id, c_id, ol_cnt, rbk;
		int returnValue = 0;
		int all_local = 0;
		int notfound = MAXITEMS + 1;
		double latency;
		long beginTime, endTime;

		int[] itemid = new int[MAX_NUM_ITEMS];
		int[] supware = new int[MAX_NUM_ITEMS];
		int[] qty = new int[MAX_NUM_ITEMS];

		if(num_node == 0)
			w_id = Util.randomNumber(1, num_ware);
		else
		{
			c_num = ((num_node * t_num) / num_conn); /* drop moduls */
			w_id = Util.randomNumber(1 + (num_ware * c_num) / num_node, (num_ware * (c_num + 1)) / num_node);
		}
		if(w_id < 1)
		{
			logger.error("invalid warehouse id: {}", w_id);
			return 0;
		}

		d_id = Util.randomNumber(1, DIST_PER_WARE);
		c_id = Util.nuRand(1023, 1, CUST_PER_DIST);

		ol_cnt = Util.randomNumber(5, 15);
		rbk = Util.randomNumber(1, 100);

		for(i = 0; i < ol_cnt; i++)
		{
			itemid[i] = Util.nuRand(8191, 1, MAXITEMS);
			if((i == ol_cnt - 1) && (rbk == 1))
			{
				itemid[i] = notfound;
			}
			if(ALLOW_MULTI_WAREHOUSE_TX)
			{
				if(Util.randomNumber(1, 100) != 1)
				{
					supware[i] = w_id;
				} else
				{
					supware[i] = selectRemoteWarehouse(w_id);
					all_local = 0;
				}
			} else
			{
				supware[i] = w_id;
			}
			qty[i] = Util.randomNumber(1, 10);
		}

		beginTime = System.currentTimeMillis();
		for(i = 0; i < MAX_RETRY; i++)
		{
			if(DEBUG)
				logger.debug("t_num: " + t_num + " w_id: " + w_id + " c_id: " + c_id + " ol_cnt: " + ol_cnt + " " +
						"all_local:" +
						" " +
						"" + all_local + " qty: " + Arrays.toString(qty));
			ret = newOrder.neword(t_num, w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty);
			endTime = System.currentTimeMillis();

			if(ret == 1)
			{
				logger.trace("neworder txn succedeed");
				returnValue = 1;
				latency = (double) (endTime - beginTime);
				//this.performanceCounter.setLatency(latency);

				if(DEBUG)
					logger.debug("BEFORE rt value: " + latency + " max_rt[0] value: " + max_rt[0]);

				if(latency > max_rt[0])
					max_rt[0] = latency;

				if(DEBUG)
					logger.debug("AFTER rt value: " + latency + " max_rt[0] value: " + max_rt[0]);

				RtHist.histInc(0, latency);

				if(Tpcc.counting_on)
				{
					if(DEBUG)
						logger.debug(" rt: " + latency + " RTIME_NEWORD " + RTIME_NEWORD);
					if(latency < RTIME_NEWORD)
					{
						if(DEBUG)
							logger.debug("Rt < RTIME_NEWORD");
						success[0]++;
						success2[0][t_num]++;
						this.stats.incrementSuccess();
					} else
					{
						if(DEBUG)
							logger.debug("Rt > RTIME_NEWORD");
						late[0]++;
						late2[0][t_num]++;
						this.stats.incrementSuccess();
					}
				}
			} else
			{
				logger.error("newOrder error: {}", newOrder.getLastError());
				returnValue = 0;
				if(Tpcc.counting_on)
				{

					retry[0]++;
					retry2[0][t_num]++;
				}
			}
		}

		if(Tpcc.counting_on && returnValue == 0)
		{
			retry[0]--;
			retry2[0][t_num]--;
			failure[0]++;
			failure2[0][t_num]++;
			this.stats.incrementAborts();
		}

		return returnValue;
	}

	/*
	  * prepare data and execute payment transaction
	  */
	private int doPayment(int t_num)
	{
		int c_num, byname, i, ret;
		int returnValue = 0;
		double latency;

		long beginTime, endTime;
		int w_id, d_id, c_w_id, c_d_id, c_id, h_amount;
		String c_last;

		if(num_node == 0)
			w_id = Util.randomNumber(1, num_ware);
		else
		{
			c_num = ((num_node * t_num) / num_conn); /* drop moduls */
			w_id = Util.randomNumber(1 + (num_ware * c_num) / num_node, (num_ware * (c_num + 1)) / num_node);
		}

		d_id = Util.randomNumber(1, DIST_PER_WARE);
		c_id = Util.nuRand(1023, 1, CUST_PER_DIST);
		c_last = Util.lastName(Util.nuRand(255, 0, 999));
		h_amount = Util.randomNumber(1, 5000);

		if(Util.randomNumber(1, 100) <= 60)
			byname = 1; /* select by last name */
		else
			byname = 0; /* select by customer id */

		if(ALLOW_MULTI_WAREHOUSE_TX)
		{
			if(Util.randomNumber(1, 100) <= 85)
			{
				c_w_id = w_id;
				c_d_id = d_id;
			} else
			{
				c_w_id = selectRemoteWarehouse(w_id);
				c_d_id = Util.randomNumber(1, DIST_PER_WARE);
			}
		} else
		{
			c_w_id = w_id;
			c_d_id = d_id;
		}

		beginTime = System.currentTimeMillis();

		for(i = 0; i < MAX_RETRY; i++)
		{
			ret = payment.payment(t_num, w_id, d_id, byname, c_w_id, c_d_id, c_id, c_last, h_amount);
			// clk2 = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tbuf2 );
			endTime = System.currentTimeMillis();

			if(ret >= 1)
			{
				logger.trace("payment txn succedeed");
				returnValue = 1;

				latency = (double) (endTime - beginTime);
				//this.performanceCounter.setLatency(latency);
				if(latency > max_rt[1])
					max_rt[1] = latency;

				RtHist.histInc(1, latency);
				if(Tpcc.counting_on)
				{
					if(latency < RTIME_PAYMENT)
					{
						success[1]++;
						success2[1][t_num]++;
						latencies[t_num] += latency;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					} else
					{
						late[1]++;
						late2[1][t_num]++;
						latencies[t_num] += latency;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					}
				}
			} else
			{
				logger.error("payment error: {}", payment.getLastError());
				returnValue = 0;
				if(Tpcc.counting_on)
				{
					retry[1]++;
					retry2[1][t_num]++;
				}
			}
		}

		if(Tpcc.counting_on && returnValue == 0)
		{
			retry[1]--;
			retry2[1][t_num]--;
			failure[1]++;
			failure2[1][t_num]++;
			this.stats.incrementAborts();
		}

		return returnValue;
	}

	/*
	  * prepare data and execute order status transaction
	  */
	private int doOrdstat(int t_num)
	{
		int c_num = 0;
		int byname = 0;
		int i = 0;
		int ret = 0;
		int returnValue = 0;
		double rt = 0.0;
		long beginTime = 0;
		long endTime = 0;

		int w_id = 0;
		int d_id = 0;
		int c_id = 0;
		String c_last = null;

		if(num_node == 0)
		{
			w_id = Util.randomNumber(1, num_ware);
		} else
		{
			c_num = ((num_node * t_num) / num_conn); /* drop moduls */
			w_id = Util.randomNumber(1 + (num_ware * c_num) / num_node, (num_ware * (c_num + 1)) / num_node);
		}
		d_id = Util.randomNumber(1, DIST_PER_WARE);
		c_id = Util.nuRand(1023, 1, CUST_PER_DIST);
		c_last = Util.lastName(Util.nuRand(255, 0, 999));
		if(Util.randomNumber(1, 100) <= 60)
		{
			byname = 1; /* select by last name */
		} else
		{
			byname = 0; /* select by customer id */
		}

		//clk1 = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tbuf1 );
		beginTime = System.currentTimeMillis();
		for(i = 0; i < MAX_RETRY; i++)
		{
			ret = orderStat.ordStat(t_num, w_id, d_id, byname, c_id, c_last);
			// clk2 = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tbuf2 );
			endTime = System.currentTimeMillis();

			if(ret >= 1)
			{
				logger.trace("orderstat txn succedeed");
				returnValue = 1;
				rt = (double) (endTime - beginTime);
				//this.performanceCounter.setLatency(rt);
				if(rt > max_rt[2])
					max_rt[2] = rt;
				RtHist.histInc(2, rt);
				if(Tpcc.counting_on)
				{
					if(rt < RTIME_ORDSTAT)
					{
						//success[2]++;
						success2[2][t_num]++;
						latencies[t_num] += rt;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					} else
					{
						late[2]++;
						late2[2][t_num]++;
						latencies[t_num] += rt;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					}
				}
			} else
			{
				logger.error("orderstat error: {}", orderStat.getLastError());
				returnValue = 0;

				if(Tpcc.counting_on)
				{
					retry[2]++;
					retry2[2][t_num]++;
				}

			}
		}

		if(Tpcc.counting_on && returnValue == 0)
		{
			retry[2]--;
			retry2[2][t_num]--;
			failure[2]++;
			failure2[2][t_num]++;
			this.stats.incrementAborts();
		}

		return returnValue;
	}

	/*
	  * execute delivery transaction
	  */
	private int doDelivery(int t_num)
	{
		int c_num = 0;
		int i = 0;
		int ret = 0;
		int returnValue = 0;
		double rt = 0.0;
		long beginTime = 0;
		long endTime = 0;
		int w_id = 0;
		int o_carrier_id = 0;

		if(num_node == 0)
		{
			w_id = Util.randomNumber(1, num_ware);
		} else
		{
			c_num = ((num_node * t_num) / num_conn); /* drop moduls */
			w_id = Util.randomNumber(1 + (num_ware * c_num) / num_node, (num_ware * (c_num + 1)) / num_node);
		}
		o_carrier_id = Util.randomNumber(1, 10);

		beginTime = System.currentTimeMillis();
		for(i = 0; i < MAX_RETRY; i++)
		{
			ret = delivery.delivery(w_id, o_carrier_id);
			endTime = System.currentTimeMillis();
			if(ret >= 1)
			{
				logger.trace("delivery txn succedeed");
				returnValue = 1;
				rt = (double) (endTime - beginTime);
				//this.performanceCounter.setLatency(rt);
				if(rt > max_rt[3])
					max_rt[3] = rt;
				RtHist.histInc(3, rt);
				if(Tpcc.counting_on)
				{
					if(rt < RTIME_DELIVERY)
					{
						success[3]++;
						success2[3][t_num]++;
						latencies[t_num] += rt;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					} else
					{
						late[3]++;
						late2[3][t_num]++;
						latencies[t_num] += rt;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					}
				}
			} else
			{
				logger.error("delivery error: {}", delivery.getLastError());
				returnValue = 0;
				if(Tpcc.counting_on)
				{
					retry[3]++;
					retry2[3][t_num]++;
				}
			}
		}

		if(Tpcc.counting_on && returnValue == 0)
		{
			//retry[3]--;
			//retry2[3][t_num]--;
			failure[3]++;
			failure2[3][t_num]++;
			this.stats.incrementAborts();

		}

		return returnValue;
	}

	/*
	  * prepare data and execute the stock level transaction
	  */
	private int doSlev(int t_num)
	{
		int c_num = 0;
		int i = 0;
		int ret = 0;
		int returnValue = 0;
		double rt = 0.0;
		long beginTime = 0;
		long endTime = 0;
		int w_id = 0;
		int d_id = 0;
		int level = 0;

		if(num_node == 0)
		{
			w_id = Util.randomNumber(1, num_ware);
		} else
		{
			c_num = ((num_node * t_num) / num_conn); /* drop moduls */
			w_id = Util.randomNumber(1 + (num_ware * c_num) / num_node, (num_ware * (c_num + 1)) / num_node);
		}
		d_id = Util.randomNumber(1, DIST_PER_WARE);
		level = Util.randomNumber(10, 20);

		// clk1 = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tbuf1 );
		beginTime = System.currentTimeMillis();
		for(i = 0; i < MAX_RETRY; i++)
		{
			ret = slev.slev(t_num, w_id, d_id, level);
			//clk2 = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tbuf2 );
			endTime = System.currentTimeMillis();

			if(ret >= 1)
			{
				logger.trace("slev txn succedeed");
				returnValue = 1;
				rt = (double) (endTime - beginTime);
				//this.performanceCounter.setLatency(rt);
				if(rt > max_rt[4])
					max_rt[4] = rt;
				RtHist.histInc(4, rt);
				if(Tpcc.counting_on)
				{
					if(rt < RTIME_SLEV)
					{
						success[4]++;
						success2[4][t_num]++;
						latencies[t_num] += rt;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					} else
					{
						late[4]++;
						late2[4][t_num]++;
						latencies[t_num] += rt;
						this.stats.incrementSuccess();
						this.stats.addLatency(endTime - beginTime);
					}
				}
			} else
			{
				logger.error("slev error: {}", slev.getLastError());
				returnValue = 0;

				if(Tpcc.counting_on)
				{
					retry[4]++;
					retry2[4][t_num]++;
				}
			}
		}

		if(Tpcc.counting_on && returnValue == 0)
		{
			retry[4]--;
			retry2[4][t_num]--;
			failure[4]++;
			failure2[4][t_num]++;
			this.stats.incrementAborts();
		}
		return returnValue;
	}

	/*
	  * produce the id of a valid warehouse other than home_ware
	  * (assuming there is one)
	  */
	private int selectRemoteWarehouse(int home_ware)
	{
		int tmp;

		if(num_ware == 1)
			return home_ware;
		while((tmp = Util.randomNumber(1, num_ware)) == home_ware)
			;
		return tmp;
	}

}
