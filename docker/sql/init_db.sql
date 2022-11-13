USE db;

CREATE TABLE `Author` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE TABLE `Source` (
  `name` varchar(20) NOT NULL,
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `Article` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `timestamp` datetime NOT NULL,
  `description` varchar(300) NOT NULL,
  `title` varchar(100) NOT NULL,
  `source_id` bigint(20) unsigned NOT NULL,
  `author_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

