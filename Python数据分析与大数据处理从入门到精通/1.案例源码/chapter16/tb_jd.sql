/*
Navicat MySQL Data Transfer

Source Server         : localVM
Source Server Version : 50724
Source Host           : 192.168.239.138:3306
Source Database       : sparktest

Target Server Type    : MYSQL
Target Server Version : 50724
File Encoding         : 65001

Date: 2018-12-31 09:21:37
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for tb_jd
-- ----------------------------
DROP TABLE IF EXISTS `tb_jd`;
CREATE TABLE `tb_jd` (
  `brand` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `sales` int(11) DEFAULT NULL,
  `price` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of tb_jd
-- ----------------------------
INSERT INTO `tb_jd` VALUES ('三星', '1800', '2799');
INSERT INTO `tb_jd` VALUES ('Apple iPhone X', '4047', '6499');
INSERT INTO `tb_jd` VALUES ('荣耀9i', '3380', '1199');
INSERT INTO `tb_jd` VALUES ('OPPO R17', '8722', '2799');
INSERT INTO `tb_jd` VALUES ('小米8', '13400', '2299');
INSERT INTO `tb_jd` VALUES ('荣耀 V10', '7780', '2799');
INSERT INTO `tb_jd` VALUES ('华为 Mate 20', '6690', '4999');
INSERT INTO `tb_jd` VALUES ('小米Play', '7780', '1099');
INSERT INTO `tb_jd` VALUES ('诺基亚 X7', '2966', '1699');
INSERT INTO `tb_jd` VALUES ('努比亚', '1228', '3199');
