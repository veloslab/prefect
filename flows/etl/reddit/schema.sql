CREATE TABLE IF NOT EXISTS prefect.reddit_new_submissions (
  `id` VARCHAR(20),
  `subreddit` VARCHAR(255),
  `title` TEXT,
  `posted` DATETIME,
  `notify` TINYINT DEFAULT 0 COMMENT 'If True, notification for this submission has been sent',
  `timestamp` timestamp DEFAULT CURRENT_TIMESTAMP,
  INDEX(`posted`),
  INDEX(`notify`),
  PRIMARY KEY(`id`)
);
