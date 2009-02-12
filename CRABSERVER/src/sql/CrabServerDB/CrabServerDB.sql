ALTER TABLE we_Job CHANGE COLUMN status status ENUM("register","released","create","submit","inProgress","finished","reallyFinished","failed","Killing","Submitting") DEFAULT 'register';

CREATE TABLE tt_taskInstance (
     id int NOT NULL AUTO_INCREMENT,
     task_name varchar(255) NOT NULL DEFAULT '',
     e_mail varchar(255) NOT NULL DEFAULT '',
     treshold_level int(3) UNSIGNED NOT NULL DEFAULT '100',
     notification_sent int(1) UNSIGNED NOT NULL DEFAULT '0' COMMENT '0=not sent; 1=sent intermediate; 2:archived' CHECK(VALUE>=0 AND VALUE<=2),
     ended_level int(3) UNSIGNED NOT NULL DEFAULT '0',
     proxy varchar(255) NOT NULL DEFAULT '',
     uuid varchar(255) NOT NULL DEFAULT '',
     status varchar(255) NOT NULL DEFAULT 'not submitted',
     work_status TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=free task; 1=busy task; 2:controlled task' CHECK(VALUE>=0 AND VALUE<=2),
     user_name varchar(255) NOT NULL DEFAULT '',
     lastupdate_time TIMESTAMP DEFAULT 0 COMMENT 'updated when task is polled',
     ended_time TIMESTAMP DEFAULT 0 COMMENT 'updated whentask archived',
     land_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'written @first insert, never updated',
     primary key(id),
     unique(task_name),
     key(task_name)
) TYPE = InnoDB DEFAULT CHARSET=latin1;
