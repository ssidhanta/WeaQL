DROP FUNCTION IF EXISTS maxClock;
DELIMITER //
CREATE FUNCTION maxClock(currentClock CHAR(100), newClock CHAR(100)) 
RETURNS CHAR(200) DETERMINISTIC
BEGIN
DECLARE returnValue CHAR(200);
SET @returnValue = '';

IF(currentClock IS NULL) then
    RETURN 'NULL';
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

    IF(@currEntry >= @newEntry) then
        SET @returnValue = CONCAT(@returnValue, @currEntry);
    ELSE
        SET @returnValue = CONCAT(@returnValue, @newEntry);
    END IF;

    IF (LOCATE('-', currentClock) = 0) then
        LEAVE loopTag;
    END IF;

    SET @returnValue = CONCAT(@returnValue, '-');
	SET currentClock = SUBSTRING(currentClock, LOCATE('-', currentClock) + 1);
	SET newClock = SUBSTRING(newClock, LOCATE('-', newClock) + 1);
END WHILE;    
    RETURN @returnValue;
END //
DELIMITER ;


-- select maxClock('2-1', '1-0');
-- select * from t1 where (select testFunc('2-0-1', '1-0-0') = 1) limit 5;