CREATE DATABASE `db_cmdb_cobra`;

USE `db_cmdb_cobra`;

CREATE TABLE `t_positions` (
  `service_id` int(11) NOT NULL COMMENT '从库id',
  `binlog_file` varchar(255) NOT NULL DEFAULT '' COMMENT 'binlog文件',
  `binlog_position` bigint(20) NOT NULL DEFAULT '0' COMMENT 'binlog文件中的位置',
  `gtid` varchar(255) NOT NULL DEFAULT '',
  `desc` varchar(255) NOT NULL DEFAULT '' COMMENT '说明',
  PRIMARY KEY (`service_id`),
  UNIQUE KEY `service_id_UNIQUE` (`service_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='用于存储跟踪位置';

CREATE DATABASE `db_test`;

USE `db_test`;

CREATE TABLE `t_test` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `device_sn` varchar(255) NOT NULL DEFAULT '' COMMENT '设备SN\n',
  `height` int(11) NOT NULL DEFAULT '0' COMMENT '高度',
  `arrival_at` timestamp NOT NULL DEFAULT '2000-01-01 00:00:00' COMMENT '设备到货时间',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT '2000-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `deleted_at` timestamp NOT NULL DEFAULT '2000-01-01 00:00:00' COMMENT '删除时间',
  `username` varchar(45) NOT NULL DEFAULT '' COMMENT '操作人',
  `action_id` varchar(64) NOT NULL DEFAULT '' COMMENT '操作id',
  `is_delete` tinyint(1) NOT NULL DEFAULT '0' COMMENT '软删除标记',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_device_sn` (`device_sn`),
  KEY `idx_device_sn` (`device_sn`),
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='用于存储跟踪位置';

