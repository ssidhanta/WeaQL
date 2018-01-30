package applications;


import java.util.Random;


/**
 * Created by dnlopes on 05/06/15.
 */
public class GeneratorUtils
{

	private static final Random RANDOM = new Random(System.nanoTime());

	private static final int C_255 = randomNumberIncludeBoundaries(0, 255);
	private static final int C_1023 = randomNumberIncludeBoundaries(0, 1023);
	private static final int C_8191 = randomNumberIncludeBoundaries(0, 8191);

	public static String makeAlphaString(int x, int y)
	{
		String str = null;
		String temp = "0123456789" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz";
		char[] alphanum = temp.toCharArray();
		int arrmax = 61;  /* index of last array element */
		int i;
		int len;
		len = randomNumber(x, y);

		for(i = 0; i < len; i++)
			if(str != null)
			{
				str = str + alphanum[randomNumber(0, arrmax)];
			} else
			{
				str = "" + alphanum[randomNumber(0, arrmax)];
			}

		return str;

	}

	/**
	 * @param min
	 * @param max
	 *
	 * @return returns a value between min (inclusive) and max (exclusive)
	 */
	public static int randomNumber(int min, int max)
	{
		return RANDOM.nextInt(max - min) + min;
	}

	/**
	 * @param min
	 * @param max
	 *
	 * @return returns a value between min (inclusive) and max (inclusive)
	 */
	public static int randomNumberIncludeBoundaries(int min, int max)
	{
		return randomNumber(min, max + 1);
	}

	public static int nuRand(int A, int x, int y)
	{
		int C = 0;

		switch(A)
		{
		case 255:
			C = C_255;
			break;
		case 1023:
			C = C_1023;
			break;
		case 8191:
			C = C_8191;
			break;
		default:
			System.out.println("NURand: unexpected value");
			System.exit(1);
		}

		return ((((randomNumberIncludeBoundaries(0, A) | randomNumberIncludeBoundaries(x, y)) + C) % (y - x + 1)) + x);

	}

	public static String lastName(int num) {
		String name = null;
		String[] n =
				{"BAR", "OUGHT", "ABLE", "PRI", "PRES",
						"ESE", "ANTI", "CALLY", "ATION", "EING"};

		name = n[num / 100];
		name = name + n[(num / 10) % 10];
		name = name + n[num % 10];

		return name;
	}
}
