#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
set -ex
hive_dir="$(dirname "${BASH_SOURCE-$0}")"
hive_dir="$(cd "${hive_dir}">/dev/null; pwd)"

# Environment variables definition
HADOOP_VERSION=${HADOOP_VERSION:-"2.7.3"}
HIVE_VERSION=${HIVE_VERSION:-"2.3.9"}
MYSQL_JDBC_DRIVER_VERSION=${MYSQL_VERSION:-"8.0.15"}
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION:-"3.4.13"}
RANGER_VERSION=${RANGER_VERSION:-"2.4.0"} # NOTE: Currently only tested Ranger 2.4.0 in the Hadoop 2.7.3 and Hive 2.3.9

HADOOP_PACKAGE_NAME="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_DOWNLOAD_URL="https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE_NAME}"

HIVE_PACKAGE_NAME="apache-hive-${HIVE_VERSION}-bin.tar.gz"
HIVE_DOWNLOAD_URL="https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_PACKAGE_NAME}"

JDBC_DIVER_PACKAGE_NAME="mysql-connector-java-${MYSQL_JDBC_DRIVER_VERSION}.tar.gz"
JDBC_DIVER_DOWNLOAD_URL="https://downloads.mysql.com/archives/get/p/3/file/${JDBC_DIVER_PACKAGE_NAME}"

ZOOKEEPER_PACKAGE_NAME="zookeeper-${ZOOKEEPER_VERSION}.tar.gz"
ZOOKEEPER_DOWNLOAD_URL="https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/${ZOOKEEPER_PACKAGE_NAME}"

RANGER_HIVE_PACKAGE_NAME="ranger-${RANGER_VERSION}-hive-plugin.tar.gz"
RANGER_HIVE_DOWNLOAD_URL=https://github.com/datastrato/apache-ranger/releases/download/release-ranger-${RANGER_VERSION}/ranger-${RANGER_VERSION}-hive-plugin.tar.gz

RANGER_HDFS_PACKAGE_NAME="ranger-${RANGER_VERSION}-hdfs-plugin.tar.gz"
RANGER_HDFS_DOWNLOAD_URL=https://github.com/datastrato/apache-ranger/releases/download/release-ranger-${RANGER_VERSION}/ranger-${RANGER_VERSION}-hdfs-plugin.tar.gz

# Prepare download packages
if [[ ! -d "${hive_dir}/packages" ]]; then
  mkdir -p "${hive_dir}/packages"
fi

if [ ! -f "${hive_dir}/packages/${HADOOP_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HADOOP_PACKAGE_NAME}" ${HADOOP_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${HIVE_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${HIVE_PACKAGE_NAME}" ${HIVE_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${JDBC_DIVER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${JDBC_DIVER_PACKAGE_NAME}" ${JDBC_DIVER_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${ZOOKEEPER_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${ZOOKEEPER_PACKAGE_NAME}" ${ZOOKEEPER_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${RANGER_HDFS_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${RANGER_HDFS_PACKAGE_NAME}" ${RANGER_HDFS_DOWNLOAD_URL}
fi

if [ ! -f "${hive_dir}/packages/${RANGER_HIVE_PACKAGE_NAME}" ]; then
  curl -L -s -o "${hive_dir}/packages/${RANGER_HIVE_PACKAGE_NAME}" ${RANGER_HIVE_DOWNLOAD_URL}
fi
