SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;


drop database if exists micro;
create database micro;
use micro;

commit;

CREATE TABLE t1 (
    a int not null,
    b int not null,
    c int not null,
    d int not null,
    e varchar(50),
    PRIMARY KEY (a)
) ENGINE=InnoDB;

CREATE TABLE t2 (
    a int not null,
    b int not null,
    c int not null,
    d int not null,
    e varchar(50),
    PRIMARY KEY (a)
) ENGINE=InnoDB;


CREATE TABLE t3 (
    a int not null,
    b int not null,
    c int not null,
    d int not null,
    e varchar(50),
    PRIMARY KEY (a)
) ENGINE=InnoDB;


CREATE TABLE t4 (
    a int not null,
    b int not null,
    c int not null,
    d int not null,
    e varchar(50),
    PRIMARY KEY (a)
) ENGINE=InnoDB;


CREATE TABLE t5 (
    a int not null,
    b int not null,
    c int not null,
    d int not null,
    e varchar(50),
    PRIMARY KEY (a)
) ENGINE=InnoDB;


commit;


CREATE INDEX t2_index_B ON t2 (b);
CREATE INDEX t4_index_B ON t4 (b);

commit;


ALTER TABLE t2 ADD CONSTRAINT t2_fkey_district_1 FOREIGN KEY(b) REFERENCES t1(a) ON DELETE CASCADE;
ALTER TABLE t4 ADD CONSTRAINT t4_fkey_district_1 FOREIGN KEY(b) REFERENCES t1(a) ON DELETE CASCADE;

commit;



SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


commit;
