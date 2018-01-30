package weaql.common.database.constraints.check;


import weaql.common.database.constraints.AbstractConstraint;
import weaql.common.database.constraints.ConstraintType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weaql.common.util.RuntimeUtils;
import weaql.common.util.ExitCode;


/**
 * Created by dnlopes on 24/03/15.
 */
public class CheckConstraint extends AbstractConstraint
{

	protected static final Logger LOG = LoggerFactory.getLogger(CheckConstraint.class);

	private int fieldType; // 1: string, 2: int, 3: float, 4: double
	protected CheckConstraintType conditionType;
	protected String thresholdValue;
	private boolean equalFlag;

	public CheckConstraint(CheckConstraintType type, String threshold, int fieldType, boolean equalFlag)
	{
		super(ConstraintType.CHECK, true);

		this.fieldType = fieldType;
		this.conditionType = type;
		this.thresholdValue = threshold;
		this.equalFlag = equalFlag;
	}

	/**
	 * Check if the argument passed is a valid value for this check constraint
	 * @param value
	 * @return
	 */
	public boolean isValidValue(String value)
	{
		double doubleValue = Double.parseDouble(value);

		if(this.fieldType == 1)
			return this.isValidString(value);
		if(this.fieldType == 2)
			return this.isValidInt((int) doubleValue);
		if(this.fieldType == 3)
			return this.isValidFloat((float) doubleValue);
		if(this.fieldType == 4)
			return this.isValidDouble(doubleValue);
		else
		{
			LOG.warn("unexpected field type");
			return false;
		}
	}

	/**
	 * For a given check constraint, this operation verifies
	 * if is it safe to update the field for newValue
	 * If it not the update must be coordinated
	 * @param newValue
	 * @param oldValue
	 * @return
	 */
	public boolean mustCoordinate(String newValue, String oldValue)
	{
		double oldDoubleValue = Double.parseDouble(oldValue);
		double newDoubleValue = Double.parseDouble(newValue);

		double delta = newDoubleValue - oldDoubleValue;

		if(this.conditionType == CheckConstraintType.LESSER)
			return delta > 0;
		else if(this.conditionType == CheckConstraintType.GREATER)
			return delta < 0;
		else
		{
			LOG.error("unexpected condition type");
			RuntimeUtils.throwRunTimeException("tried to verify an unexpected check constraint", ExitCode.UNEXPECTED_OP);
			return false;
		}
	}

	/**
	 * Calculates
	 * @param newValue
	 * @param oldValue
	 * @return
	 */
	public String calculateDelta(String newValue, String oldValue)
	{
		double newValueDouble = Double.parseDouble(newValue);
		double oldValueDouble = Double.parseDouble(oldValue);

		double delta = newValueDouble - oldValueDouble;
		return String.valueOf(delta);
	}

	private boolean isValidInt(int value)
	{
		switch(this.conditionType)
		{
		case LESSER:
			if(equalFlag)
				return value <= Integer.parseInt(this.thresholdValue);
			else
				return value < Integer.parseInt(this.thresholdValue);
		case GREATER:
			if(equalFlag)
				return value >= Integer.parseInt(this.thresholdValue);
			else
				return value > Integer.parseInt(this.thresholdValue);
		case EQUAL:
			return value == Integer.parseInt(this.thresholdValue);
		case NOT_EQUAL:
			return value != Integer.parseInt(this.thresholdValue);
		default:
			LOG.warn("unexpected condition type");
			return false;
		}

	}

	private boolean isValidFloat(float value)
	{
		switch(this.conditionType)
		{
		case LESSER:
			if(equalFlag)
				return value <= Float.parseFloat(this.thresholdValue);
			else
				return value < Float.parseFloat(this.thresholdValue);
		case GREATER:
			if(equalFlag)
				return value >= Float.parseFloat(this.thresholdValue);
			else
				return value > Float.parseFloat(this.thresholdValue);
		case EQUAL:
			return value == Float.parseFloat(this.thresholdValue);
		case NOT_EQUAL:
			return value != Float.parseFloat(this.thresholdValue);
		default:
			LOG.warn("unexpected condition type");
			return false;
		}
	}

	private boolean isValidDouble(double value)
	{
		switch(this.conditionType)
		{
		case LESSER:
			if(equalFlag)
				return value <= Double.parseDouble(this.thresholdValue);
			else
				return value < Double.parseDouble(this.thresholdValue);
		case GREATER:
			if(equalFlag)
				return value >= Double.parseDouble(this.thresholdValue);
			else
				return value > Double.parseDouble(this.thresholdValue);
		case EQUAL:
			return value == Double.parseDouble(this.thresholdValue);
		case NOT_EQUAL:
			return value != Double.parseDouble(this.thresholdValue);
		default:
			LOG.warn("unexpected condition type");
			return false;
		}
	}

	private boolean isValidString(String value)
	{
		switch(this.conditionType)
		{
		case LESSER:
			if(equalFlag)
				return value.compareTo(this.thresholdValue) <= 0;
			else
				return value.compareTo(this.thresholdValue) < 0;
		case GREATER:
			if(equalFlag)
				return value.compareTo(this.thresholdValue) >= 0;
			else
				return value.compareTo(this.thresholdValue) > 0;
		case EQUAL:
			return this.thresholdValue.compareTo(value) == 0;
		case NOT_EQUAL:
			return this.thresholdValue.compareTo(value) != 0;
		default:
			LOG.warn("unexpected condition type");
			return false;
		}

	}

}
