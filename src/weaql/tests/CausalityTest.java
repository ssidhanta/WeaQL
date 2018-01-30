package weaql.tests;


import weaql.server.util.LogicalClock;


/**
 * Created by dnlopes on 23/05/15.
 */
public class CausalityTest
{

	public static void main(String args[])
	{
		LogicalClock r1_clock1 = new LogicalClock("1-0-0");
		LogicalClock r1_clock2 = new LogicalClock("2-0-0");

		LogicalClock r3_clock1 = new LogicalClock("0-0-1");
		LogicalClock r3_clock2 = new LogicalClock("0-0-2");
		LogicalClock r3_clock3 = new LogicalClock("1-0-3");

		LogicalClock r2_clock1 = new LogicalClock(r1_clock1.getDcEntries());
		LogicalClock r2_clock2 = null;
		LogicalClock r2_clock3 = null;
		LogicalClock r2_clock4 = null;
		LogicalClock r2_clock5 = null;


		if(r2_clock1.lessThanByAtMostOne(r3_clock1))
			r2_clock2 = r2_clock1.maxClock(r3_clock1);

		if(r2_clock2.lessThanByAtMostOne(r3_clock2))
			r2_clock3 = r2_clock2.maxClock(r3_clock2);

		if(r2_clock3.lessThanByAtMostOne(r3_clock3))
			r2_clock4 = r2_clock3.maxClock(r3_clock3);

		if(r2_clock4.lessThanByAtMostOne(r1_clock2))
			r2_clock5 = r2_clock4.maxClock(r1_clock2);

	}

}
