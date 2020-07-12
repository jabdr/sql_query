BEGIN;
DROP TABLE IF EXISTS `test`;

CREATE TABLE `test` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `col1` VARCHAR(255),
    `col2` INTEGER,
    `col3` DATETIME,
    `col4` DATE,
    `col5` BIGINT,
    `col6` TEXT,
    `col7` INTEGER(1)
);

COMMIT;