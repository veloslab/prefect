CREATE TABLE `prefect`.`microcenter_open_box` (
  `id` int unsigned NOT NULL,
  `store` varchar(50) NOT NULL,
  `category` varchar(50) NOT NULL,
  `name` text,
  `price` decimal(12,2) DEFAULT NULL,
  `available` tinyint DEFAULT 1,
  `notify` tinyint DEFAULT 0,
  `updated` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created` timestamp DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`, `store`, `category`));

CREATE INDEX idx_available ON prefect.microcenter_open_box(store, category, available);
CREATE INDEX idx_notify ON prefect.microcenter_open_box(notify, available);
