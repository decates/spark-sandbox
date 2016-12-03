#!/usr/bin/env bash
# Exit on failure
set -e

# Make sure only root can run our script
if [[ $EUID -eq 0 ]]; then
   echo "This script must NOT be run as root" 1>&2
   exit 1
fi

BASE_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SPARK_VERSION=2.0.2
HADOOP_VERSION=2.7
JDK_VERSION=8
SBT_VERSION=0.13.13
SPARKLING_WATER_MAJOR_VERSION=1.5
SPARKLING_WATER_MINOR_VERSION=6

SPARK_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARKLING_WATER_NAME=sparkling-water-$SPARKLING_WATER_MAJOR_VERSION.$SPARKLING_WATER_MINOR_VERSION

# Update package cache
sudo apt-get update

# Download and unpack
# 1. Apache Spark
wget http://www.apache.org/dyn/closer.lua/spark/spark-$SPARK_VERSION/$SPARK_NAME.tgz
tar xvzf $SPARK_NAME.tgz

# 3. SBT
wget https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz
tar xvzf sbt-$SBT_VERSION.tgz

# 4. h2o Sparkling Water
wget http://h2o-release.s3.amazonaws.com/sparkling-water/rel-$SPARKLING_WATER_MAJOR_VERSION/$SPARKLING_WATER_MINOR_VERSION/$SPARKLING_WATER_NAME.zip
unzip $SPARKLING_WATER_NAME.zip

# Install Java JDK
sudo apt-get install openjdk-$JDK_VERSION-jdk

# Configure SSH
sudo apt-get install openssh-server
cd ~
ssh-keygen -t rsa -P ""
cat ./.ssh/id_rsa.pub >> ./.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
sudo service ssh restart

# Configure IPv6
sudo cat <<EOT1 >> /etc/sysctl.conf
net.ipv6.conf.all.disable_ipv6 = 1 
net.ipv6.conf.default.disable_ipv6 = 1
net.ipv6.conf.lo.disable_ipv6 = 1
EOT1

# Configure Spark configuration
cd $BASE_PATH/$SPARK_NAME/conf
cp spark-env.sh.template spark-env.sh
cat <<EOT2 >> spack-env.sh
JAVA_HOME=/usr/lib/jvm/java-$JDK_VERSION-openjdk-amd64/
SPARK_MASTER_IP=127.0.0.1
SPARK_WORKER_MEMORY=4g
EOT2

# Configure .bashrc
cat <<EOT3 >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-$JDK_VERSION-openjdk-amd64/
export SBT_HOME=$BASE_PATH/sbt-$SBT_VERSION/
export SPARK_HOME=$BASE_PATH/$SPARK_NAME/
export PATH=$PATH:$JAVA_HOME/bin
export PATH=$PATH:$SBT_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
EOT3

echo Done.
echo "Please load a new shell to use spark..."
