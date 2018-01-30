package weaql.server.util;


import weaql.common.util.RuntimeUtils;
import weaql.common.util.ExitCode;


public class LogicalClock implements java.io.Serializable
{

	public static final String CLOCK_PLACEHOLLDER_WITH_ESCAPED_CHARS = "@clock@";
	public static final String CLOCK_PLACEHOLDER = "@clock@";

	private long[] entries;

	public LogicalClock(int entriesNumber)
	{
		this.entries = new long[entriesNumber];
		for(int i = 0; i < entriesNumber; i++)
		{
			this.entries[i] = 0;
		}
	}

	public LogicalClock(String clockString)
	{
		String[] tmpEntries = clockString.split("-");
		this.entries = new long[tmpEntries.length];

		for(int i = 0; i < tmpEntries.length; i++)
		{
			this.entries[i] = Long.parseLong(tmpEntries[i]);
		}
	}

	public LogicalClock(long[] entries)
	{
		this.entries = new long[entries.length];

		for(int i = 0; i < entries.length; i++)
			this.entries[i] = entries[i];
	}


	public long[] getDcEntries()
	{
		return this.entries;
	}

	public long getEntry(int index)
	{
		return this.entries[index];
	}

	public boolean comparable(LogicalClock lc)
	{
		return lc.entries.length == this.entries.length;
	}

	/**
	 * @param lc
	 *
	 * @return a logical clock that is the pairwise maximum of these two clocks
	 */
	public LogicalClock maxClock(LogicalClock lc)
	{
		if(!comparable(lc))
			RuntimeUtils.throwRunTimeException(
					"incomparable logicalclocks: " + entries.length + " " + lc.entries.length, ExitCode.INVALIDUSAGE);

		long[] tmpEntries = new long[entries.length];

		for(int i = 0; i < tmpEntries.length; i++)
		{
			if(this.entries[i] >= lc.entries[i])
				tmpEntries[i] = this.entries[i];
			else
				tmpEntries[i] = lc.entries[i];
		}

		return new LogicalClock(tmpEntries);
	}

	public void increment(int index)
	{
		this.entries[index]++;
	}

	public boolean precedes(LogicalClock lc)
	{
		boolean res = false;

		for(int i = 0; res && i < entries.length; i++)
		{
			res = entries[i] <= lc.entries[i];
		}

		return res;
	}

	public boolean precededBy(LogicalClock lc)
	{
		return lc.precedes(this);
	}

	public boolean equals(LogicalClock lc)
	{
		boolean res = true;

		for(int i = 0; res && i < entries.length; i++)
		{
			res = res && entries[i] == lc.entries[i];
		}
		return res;
	}

	/**
	 * returns true iff this is less than lc in at most 1 position and by at most 1.
	 * returns false otherwise
	 */
	public boolean lessThanByAtMostOne(LogicalClock lc)
	{
		boolean res = comparable(lc);
		boolean one = false;

		for(int i = 0; res && i < entries.length; i++)
		{
			if(entries[i] < lc.entries[i])
			{
				if(entries[i] == lc.entries[i] - 1)
				{
					if(!one)
					{
						one = true;
					} else
					{
						res = false;
					}
				} else
				{
					res = false;
				}
			}
		}
		return res && one;
	}

	public String getClockValue()
	{

		StringBuilder buffer = new StringBuilder();

		for(int i = 0; i < this.entries.length; i++)
		{
			buffer.append(this.entries[i]);
			if(i < this.entries.length -1)
				buffer.append("-");
		}

		return buffer.toString();
	}

	@Override
	public String toString()
	{
		StringBuilder buffer = new StringBuilder();
		buffer.append("'");
		buffer.append(getClockValue());
		buffer.append("'");
		return buffer.toString();
	}

	public int hashcode()
	{
		long sum = 0;
		for(int i = 0; i < entries.length; i++)
		{
			sum += (int) entries[i];
		}
		return (int) (sum * 1000);

	}

	public LogicalClock duplicate()
	{
		return new LogicalClock(this.entries);
	}

}