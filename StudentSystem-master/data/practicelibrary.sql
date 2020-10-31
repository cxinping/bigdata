/*
 Navicat Premium Data Transfer

 Source Server         : LocalDB
 Source Server Type    : MySQL
 Source Server Version : 80018
 Source Host           : localhost:3306
 Source Schema         : practicelibrary

 Target Server Type    : MySQL
 Target Server Version : 80018
 File Encoding         : 65001

 Date: 27/07/2020 00:05:25
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for grade
-- ----------------------------
DROP TABLE IF EXISTS `grade`;
CREATE TABLE `grade`  (
  `g_id` int(11) NOT NULL AUTO_INCREMENT,
  `g_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL,
  `g_create_time` datetime(0) NULL DEFAULT NULL,
  PRIMARY KEY (`g_id`) USING BTREE,
  UNIQUE INDEX `g_name`(`g_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of grade
-- ----------------------------
INSERT INTO `grade` VALUES (1, '一年级', '2020-07-08 20:13:57');
INSERT INTO `grade` VALUES (2, '二年级', '2020-07-16 20:24:12');
INSERT INTO `grade` VALUES (3, '三年级', '2020-07-01 20:24:22');

-- ----------------------------
-- Table structure for permission
-- ----------------------------
DROP TABLE IF EXISTS `permission`;
CREATE TABLE `permission`  (
  `p_id` int(11) NOT NULL AUTO_INCREMENT,
  `p_name` varchar(16) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL,
  `p_er` varchar(16) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL,
  PRIMARY KEY (`p_id`) USING BTREE,
  UNIQUE INDEX `p_name`(`p_name`) USING BTREE,
  UNIQUE INDEX `p_er`(`p_er`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of permission
-- ----------------------------
INSERT INTO `permission` VALUES (1, '年级管理', 'BJLB');
INSERT INTO `permission` VALUES (2, '添加年级', 'TJBJ');
INSERT INTO `permission` VALUES (3, '学生管理', 'XSLB');
INSERT INTO `permission` VALUES (4, '添加学生', 'TJXS');
INSERT INTO `permission` VALUES (5, '权限管理', 'JSLB');
INSERT INTO `permission` VALUES (6, '添加角色', 'TJJS');
INSERT INTO `permission` VALUES (7, '权限列表', 'QXLB');
INSERT INTO `permission` VALUES (8, '添加权限', 'TJQX');
INSERT INTO `permission` VALUES (9, '用户管理', 'YHLB');
INSERT INTO `permission` VALUES (10, '添加用户', 'TJYH');

-- ----------------------------
-- Table structure for role
-- ----------------------------
DROP TABLE IF EXISTS `role`;
CREATE TABLE `role`  (
  `r_id` int(11) NOT NULL AUTO_INCREMENT,
  `r_name` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL,
  PRIMARY KEY (`r_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of role
-- ----------------------------
INSERT INTO `role` VALUES (1, '超级管理员');
INSERT INTO `role` VALUES (2, '教师');

-- ----------------------------
-- Table structure for role_permission
-- ----------------------------
DROP TABLE IF EXISTS `role_permission`;
CREATE TABLE `role_permission`  (
  `role_id` int(11) NOT NULL,
  `permission_id` int(11) NOT NULL,
  PRIMARY KEY (`role_id`, `permission_id`) USING BTREE,
  INDEX `permission_id`(`permission_id`) USING BTREE,
  CONSTRAINT `role_permission_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `role` (`r_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `role_permission_ibfk_2` FOREIGN KEY (`permission_id`) REFERENCES `permission` (`p_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of role_permission
-- ----------------------------
INSERT INTO `role_permission` VALUES (1, 1);
INSERT INTO `role_permission` VALUES (1, 2);
INSERT INTO `role_permission` VALUES (1, 3);
INSERT INTO `role_permission` VALUES (2, 3);
INSERT INTO `role_permission` VALUES (1, 4);
INSERT INTO `role_permission` VALUES (2, 4);
INSERT INTO `role_permission` VALUES (1, 5);
INSERT INTO `role_permission` VALUES (1, 6);
INSERT INTO `role_permission` VALUES (1, 7);
INSERT INTO `role_permission` VALUES (1, 8);
INSERT INTO `role_permission` VALUES (1, 9);
INSERT INTO `role_permission` VALUES (1, 10);

-- ----------------------------
-- Table structure for student
-- ----------------------------
DROP TABLE IF EXISTS `student`;
CREATE TABLE `student`  (
  `s_id` int(11) NOT NULL AUTO_INCREMENT,
  `s_name` varchar(16) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL,
  `s_sex` int(11) NULL DEFAULT NULL,
  `grade_id` int(11) NULL DEFAULT NULL,
  PRIMARY KEY (`s_id`) USING BTREE,
  UNIQUE INDEX `s_name`(`s_name`) USING BTREE,
  INDEX `grade_id`(`grade_id`) USING BTREE,
  CONSTRAINT `student_ibfk_1` FOREIGN KEY (`grade_id`) REFERENCES `grade` (`g_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of student
-- ----------------------------
INSERT INTO `student` VALUES (1, 'ZZP', 1, 1);
INSERT INTO `student` VALUES (2, 'qwe', 0, 2);
INSERT INTO `student` VALUES (3, 'eee', 1, 3);
INSERT INTO `student` VALUES (4, 'tyu', 1, 2);
INSERT INTO `student` VALUES (5, 'jkl', 0, 3);
INSERT INTO `student` VALUES (6, 'fgg', 1, 1);
INSERT INTO `student` VALUES (7, 'ddf', 0, 2);
INSERT INTO `student` VALUES (8, 'tgn', 0, 3);
INSERT INTO `student` VALUES (9, 'dad', 1, 1);
INSERT INTO `student` VALUES (10, 'khj', 1, 2);

-- ----------------------------
-- Table structure for user
-- ----------------------------
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user`  (
  `u_id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(16) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL,
  `password` varchar(250) CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL,
  `u_create_time` datetime(0) NULL DEFAULT NULL,
  `role_id` int(11) NULL DEFAULT NULL,
  PRIMARY KEY (`u_id`) USING BTREE,
  UNIQUE INDEX `username`(`username`) USING BTREE,
  INDEX `role_id`(`role_id`) USING BTREE,
  CONSTRAINT `user_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `role` (`r_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of user
-- ----------------------------
INSERT INTO `user` VALUES (1, 'asd', '123456', '2020-07-26 20:04:24', 1);
INSERT INTO `user` VALUES (2, 'qwe', '123456', '2020-07-27 00:00:00', 2);

SET FOREIGN_KEY_CHECKS = 1;
