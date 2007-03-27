use ProdAgentDB;
CREATE TABLE js_taskInstance (
     id int NOT NULL auto_increment,
     taskName varchar(255) NOT NULL default '',
     eMail varchar(255) NOT NULL default '',
     tresholdLevel int (3) UNSIGNED NOT NULL default '100',
     notificationSent int (1) NOT NULL default '0',
     endedLevel INT(3) UNSIGNED NOT NULL default '0',
     primary key(id),
     unique(taskName),
     key(taskName)
) TYPE = InnoDB DEFAULT CHARSET=latin1;
