-- 错误数据表结构（与 target_data 相同，增加错误信息字段）
CREATE TABLE IF NOT EXISTS `target_data_error` (
  `id` BIGINT NOT NULL COMMENT '主键 ID',
  `business_time` DATETIME DEFAULT NULL COMMENT '业务时间',
  `user_code` VARCHAR(100) DEFAULT NULL COMMENT '用户编码',
  `user_name` VARCHAR(200) DEFAULT NULL COMMENT '用户名称',
  `amount` DECIMAL(18,2) DEFAULT NULL COMMENT '金额',
  `status` INT DEFAULT NULL COMMENT '状态',
  `remark` VARCHAR(500) DEFAULT NULL COMMENT '备注',
  `source_flag` VARCHAR(50) DEFAULT NULL COMMENT '来源标识',
  `migrate_time` DATETIME DEFAULT NULL COMMENT '迁移时间',
  `create_time` DATETIME DEFAULT NULL COMMENT '创建时间',
  `error_msg` TEXT COMMENT '错误信息',
  `error_time` DATETIME DEFAULT NULL COMMENT '错误时间',
  PRIMARY KEY (`id`),
  KEY `idx_user_code` (`user_code`),
  KEY `idx_business_time` (`business_time`),
  KEY `idx_error_time` (`error_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据迁移错误表';
