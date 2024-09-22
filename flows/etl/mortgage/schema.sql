CREATE TABLE `prefect`.`mortgage_rates` (
  `posted` DATETIME NOT NULL,
  `bank` varchar(20) NOT NULL,
  `term` varchar(50) NOT NULL,
  `rate` decimal(6,3) DEFAULT NULL,
  `discount_points` decimal(6,3) DEFAULT NULL,
  PRIMARY KEY (`posted`, `bank`, `term`)
);
