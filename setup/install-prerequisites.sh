#!/usr/bin/env bash
echo Running as user: $USER

set -e
BASE_PATH=$HOME
SPARK_VERSION=2.0.2
HADOOP_VERSION=2.7
JDK_VERSION=8
SBT_VERSION=0.13.13
SPARKLING_WATER_MAJOR_VERSION=2.0
SPARKLING_WATER_MINOR_VERSION=0

SPARK_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARKLING_WATER_NAME=sparkling-water-$SPARKLING_WATER_MAJOR_VERSION.$SPARKLING_WATER_MINOR_VERSION
SPARKLING_HOME=$HOME/$SPARKLING_WATER_NAME

# Display environment variables
export

# Update package cache
sudo apt-get update

echo Download and unpack
echo - Apache Spark
wget -nv http://apache.mirrors.nublue.co.uk/spark/spark-$SPARK_VERSION/$SPARK_NAME.tgz
tar xzf $SPARK_NAME.tgz

echo - SBT
wget -nv https://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz
tar xzf sbt-$SBT_VERSION.tgz

echo - h2o Sparkling Water
wget -nv http://h2o-release.s3.amazonaws.com/sparkling-water/rel-$SPARKLING_WATER_MAJOR_VERSION/$SPARKLING_WATER_MINOR_VERSION/$SPARKLING_WATER_NAME.zip
sudo apt-get -qy install unzip
unzip -q $SPARKLING_WATER_NAME.zip

echo Install Java JDK
sudo apt-get -qy install openjdk-$JDK_VERSION-jdk-headless

echo Configure SSH
sudo apt-get -qy install openssh-server
cd ~
ssh-keygen -t rsa -f /tmp/sshkey -q -N ""
ssh-keygen -t rsa -P ""
mkdir -p .ssh
touch ~/.ssh/authorized_keys
cat /tmp/sshkey >> ./.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
rm /tmp/sshkey
sudo service ssh restart

# Configure IPv6
#sudo cat <<EOT1 >> /etc/sysctl.conf
#sudo echo net.ipv6.conf.all.disable_ipv6 = 1 >> /etc/sysctl.conf
#sudo echo net.ipv6.conf.default.disable_ipv6 = 1 >> /etc/sysctl.conf
#sudo echo net.ipv6.conf.lo.disable_ipv6 = 1 >> /etc/sysctl.conf
#EOT1

echo Configure Spark configuration
cd $BASE_PATH/$SPARK_NAME/conf
cp spark-env.sh.template spark-env.sh
echo "	
JAVA_HOME=/usr/lib/jvm/java-$JDK_VERSION-openjdk-amd64/
SPARK_MASTER_IP=127.0.0.1
SPARK_WORKER_MEMORY=4g
" >> spark-env.sh

echo Configure .bashrc
echo "	 
export JAVA_HOME=/usr/lib/jvm/java-$JDK_VERSION-openjdk-amd64
export SBT_HOME=$BASE_PATH/sbt-$SBT_VERSION
export SPARK_HOME=$BASE_PATH/$SPARK_NAME
" >> ~/.bashrc

echo Configure .profile
echo "

# Add Spark stuff to PATH
PATH=\$PATH:$JAVA_HOME/bin
PATH=\$PATH:$SBT_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$SPARKLING_HOME/bin
" >> ~/.profile

echo Done.