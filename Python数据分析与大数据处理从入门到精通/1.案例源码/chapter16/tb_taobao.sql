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
-- Table structure for tb_taobao
-- ----------------------------
DROP TABLE IF EXISTS `tb_taobao`;
CREATE TABLE `tb_taobao` (
  `brand` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `sales` int(11) DEFAULT NULL,
  `price` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of tb_taobao
-- ----------------------------
INSERT INTO `tb_taobao` VALUES ('三星', '3216', '2799');
INSERT INTO `tb_taobao` VALUES ('Apple iPhone X', '5580', '6499');
INSERT INTO `tb_taobao` VALUES ('荣耀9i', '4278', '1199');
INSERT INTO `tb_taobao` VALUES ('OPPO R17', '6113', '2799');
INSERT INTO `tb_taobao` VALUES ('小米8', '9227', '2299');
INSERT INTO `tb_taobao` VALUES ('荣耀 V10', '6433', '2799');
INSERT INTO `tb_taobao` VALUES ('华为 Mate 20', '7223', '4999');
INSERT INTO `tb_taobao` VALUES ('小米Play', '8916', '1099');
