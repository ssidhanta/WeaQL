package applications.util;


/**
 * Created by dnlopes on 22/10/15.
 */
public class SymbolsManager
{
	public static final String SYMBOL_PREFIX = "_SYM_";
	public static final String ONE_TIME_SYMBOL = SYMBOL_PREFIX + "ONE_TIME";
	private int symbolCounter;

	public SymbolsManager()
	{
		symbolCounter = 0;
	}

	public String getNextSymbol()
	{
		this.symbolCounter++;
		return SYMBOL_PREFIX + String.valueOf(symbolCounter);
	}

}
