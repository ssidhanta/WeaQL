-- This function is used to decide wether an update should be installed or not
-- Returns TRUE if newClock > currentClock in a deterministic way
DROP FUNCTION IF EXISTS isStrictlyGreater;
DELIMITER //
CREATE FUNCTION isStrictlyGreater(currentClock CHAR(100), newClock CHAR(100)) 
RETURNS BOOL DETERMINISTIC
BEGIN
DECLARE isConcurrent BOOL;
DECLARE isLesser BOOL;
DECLARE dumbFlag BOOL;
DECLARE isGreater BOOL;
DECLARE cycleCond BOOL;
DECLARE returnValue BOOL;
SET @dumbFlag = FALSE;
SET @returnValue = FALSE;
SET @isConcurrent = FALSE;
SET @isLesser = FALSE;
SET @isGreater = FALSE;

IF(currentClock IS NULL) then
    RETURN TRUE;
END IF;

loopTag: WHILE (TRUE) DO

    SET @index = LOCATE('-', currentClock);

    IF(@index = 0) then
        SET @currEntry = CONVERT (currentClock, SIGNED);
        SET @newEntry = CONVERT (newClock, SIGNED);
    ELSE
        SET @index = LOCATE('-', currentClock);
        SET @index2 = LOCATE('-', newClock);
        SET @currEntry = CONVERT (LEFT(currentClock, @index-1), SIGNED);
        SET @newEntry = CONVERT (LEFT(newClock, @index2-1), SIGNED);
    END IF;

    IF(@currEntry > @newEntry) then
            SET @dumbFlag = TRUE;    
    	IF(@isLesser) then
    	   SET @isConcurrent = TRUE;
    	   LEAVE loopTag;
    	END IF;
    	SET @isGreater = TRUE;

    ELSEIF(@currEntry < @newEntry) then
    	IF(@isGreater) then
    	   SET @isConcurrent = TRUE;
            IF(@dumbFlag = FALSE) then
                SET @isGreater = TRUE;
            END IF;
    	   LEAVE loopTag;
        END IF;    
        SET @isLesser = TRUE;  
    END IF;
 
	IF (LOCATE('-', currentClock) = 0) then
        LEAVE loopTag;
    END IF;

	SET currentClock = SUBSTRING(currentClock, LOCATE('-', currentClock) + 1);
	SET newClock = SUBSTRING(newClock, LOCATE('-', newClock) + 1);
END WHILE;
    IF(@isConcurrent) then
        SELECT FALSE INTO @returnValue;  
	ELSEIF(@isLesser) then		
        SELECT TRUE INTO @returnValue;
    ELSE
        SELECT FALSE INTO @returnValue;
	END IF;
    RETURN @returnValue;	
END //
DELIMITER ;


-- select isStrictlyGreater('2-1', '1-0');
-- select * from t1 where (select testFunc('2-0-1', '1-0-0') = 1) limit 5;