CREATE TABLE js_taskInstance (
     id int NOT NULL auto_increment,
     taskName varchar(255) NOT NULL default '',
     eMail varchar(255) NOT NULL default '',
     tresholdLevel int (3) UNSIGNED NOT NULL default '100',
     notificationSent int (1) NOT NULL default '0',
     endedLevel INT(3) UNSIGNED NOT NULL default '0',
     proxy varchar(255) NOT NULL default '',
     uuid varchar(255) NOT NULL default '',
     status varchar(255) NOT NULL default 'not submitted',
     work_status TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=free task; 1=busy task; 2:controlled task' CHECK(VALUE>=0 AND VALUE<=2),
     user_name varchar(255) NOT NULL default '',
     primary key(id),
     unique(taskName),
     key(taskName)
) TYPE = InnoDB DEFAULT CHARSET=latin1;
