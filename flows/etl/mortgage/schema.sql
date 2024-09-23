CREATE TABLE `prefect`.`mortgage_rates_history` (
  `posted` DATETIME NOT NULL,
  `bank` varchar(20) NOT NULL,
  `term` varchar(50) NOT NULL,
  `rate` decimal(6,3) DEFAULT NULL,
  `discount_points` decimal(6,3) DEFAULT NULL,
  PRIMARY KEY (`posted`, `bank`, `term`)
);
CREATE TABLE `prefect`.`mortgage_rates_latest` (
  `bank` varchar(20) NOT NULL,
  `term` varchar(50) NOT NULL,
  `rate` decimal(6,3) DEFAULT NULL,
  `discount_points` decimal(6,3) DEFAULT NULL,
  `posted` DATETIME NOT NULL,
  PRIMARY KEY (`bank`, `term`)
);
