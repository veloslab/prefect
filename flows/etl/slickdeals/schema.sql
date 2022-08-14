CREATE TABLE IF NOT EXISTS prefect.slickdeals_post (
  `id` INT(10) UNSIGNED AUTO_INCREMENT,
  `thread` INT NOT NULL UNIQUE,
  `category` VARCHAR(255),
  `title` TEXT,
  `posted` DATETIME,
  `notify` TINYINT DEFAULT 0,
  `timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,
  INDEX(`posted`),
  PRIMARY KEY(`id`)
);

CREATE TABLE IF NOT EXISTS prefect.report_slickdeals_post_meta (
  `id` INT(10) UNSIGNED AUTO_INCREMENT,
  `acquired` DATETIME NOT NULL UNIQUE,
  `timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(`id`)
);

CREATE TABLE IF NOT EXISTS prefect.slickdeals_post_meta (
  `report` INT(10) UNSIGNED,
  `post` INT(10) UNSIGNED,
  `age` INT,
  `comments` MEDIUMINT,
  `views` MEDIUMINT,
  `votes` MEDIUMINT,
  `score` MEDIUMINT,
  PRIMARY KEY(`report`, `post`)
);

ALTER TABLE prefect.slickdeals_post_meta
    ADD FOREIGN KEY (`report`) REFERENCES `prefect`.`report_slickdeals_post_meta` (`id`) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE prefect.slickdeals_post_meta
    ADD FOREIGN KEY (`post`) REFERENCES `prefect`.`slickdeals_post` (`id`) ON UPDATE CASCADE;
