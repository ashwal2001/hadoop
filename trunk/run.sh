#!/bin/bash

set -e

#log start time
TIMESTAMP=`date "+%Y%m%d%H%M"`
EMR_DIR=/home/ashok/data/emr
CURRENT_PATH=`pwd`
LOG_FILE=$CURRENT_PATH/run_emr_cluster.log.$TIMESTAMP

START=`date "+%Y-%m-%d %H:%M"`

echo $START > $LOG_FILE
echo "Current/Present working directory $CURRENT_PATH" >> $LOG_FILE 

SVN_USER=ashok
SVN_PASS=recos
REPO_URL=http://172.19.3.61/svn
REPO_PATH=recoengine
TARGET_PATH=trunk/target
S3_BUCKET_PATH=s3://jade-hadoop
JAR_FILE_NAME=recoengine-1.0-SNAPSHOT.jar

CERT_PATH=$EMR_DIR/jabong.pem
SSH_KEY=$EMR_DIR/jabong.pem
NAME="Hadoop Cluster Nightly"
CREDENTIALS=$EMR_DIR/credentials.json
NUM_INSTANCES=3
MASTER_INSTANCE_TYPE=m1.large
SLAVE_INSTANCE_TYPE=c1.xlarge
HADOOP_VERSION=1.0.3
AMI_VERSION=2.3

PVIEW_CSV=pview.csv
SALES_CSV=sales_order_item_4_mnths.csv

#Checkout/update code
if [ -d $CURRENT_PATH/$REPO_PATH ]; then
  SVN_CMD="svn --username $SVN_USER --password $SVN_PASS --no-auth-cache up $REPO_PATH"
else
  SVN_CMD="svn --username $SVN_USER --password $SVN_PASS --no-auth-cache checkout ${REPO_URL}/${REPO_PATH} $REPO_PATH"
fi

echo Checkout code with $SVN_CMD >> $LOG_FILE
$SVN_CMD>> $LOG_FILE 2>&1

cd $CURRENT_PATH/$REPO_PATH
SVN_STATUS=$(svn info)

echo -e "SVN Status/info\n$SVN_STATUS" >> $LOG_FILE 2>&1

cd trunk
#Build code
mvn clean package >> $LOG_FILE 2>&1

cd $CURRENT_PATH

#take export of data from mongo
#mongoexport --host localhost --port 27017 --db products --collection pview --csv -f sku,sessionId,time --out $CURRENT_PATH/$PVIEW_CSV  >> $LOG_FILE 2>&1

#copy data to s3
#s3cmd del $S3_BUCKET_PATH/$PVIEW_CSV  >> $LOG_FILE 2>&1
#s3cmd put $CURRENT_PATH/$PVIEW_CSV $S3_BUCKET_PATH/$PVIEW_CSV  >> $LOG_FILE 2>&1

#export data from mysql
## No need for this as data will be available on S3 via cron

# create emr cluster
CMD="$EMR_DIR/elastic-mapreduce -c $CREDENTIALS --create --name "$NAME" --alive --num-instances $NUM_INSTANCES --master-instance-type $MASTER_INSTANCE_TYPE --slave-instance-type $SLAVE_INSTANCE_TYPE --hadoop-version $HADOOP_VERSION --ami-version $AMI_VERSION --log-uri $S3_BUCKET_PATH/logs"
 
echo Launching EMR cluster with command $CMD >> $LOG_FILE 2>&1

JOBFLOWID=`$CMD| egrep 'j-.*' -o`

echo JOBFLOWID: $JOBFLOWID >> $LOG_FILE 2>&1

while true;  do 
    STATUS=$($EMR_DIR/elastic-mapreduce --list --jobflow $JOBFLOWID | grep WAITING)
    echo "Status of $JOBFLOWID : $STATUS" >> $LOG_FILE 2>&1
    if [ $STATUS = 0 ]; then
        break
    fi
    sleep 10
done

# Fetch master public dns from job id
MASTER_PUBLIC_DNS=`$EMR_DIR/elastic-mapreduce --jobflow $JOBFLOWID --describe | grep MasterPublicDnsName | egrep 'ec2.*com' -o`
#./elastic-mapreduce -j j-2ID3C68C4URGC --describe | grep -Po '(?<="MasterPublicDnsName": ")[^"]*'
#MASTER_PUBLIC_DNS=$(elastic-mapreduce --list | grep "$JOBID"| awk '{print $3}')

echo "Task tracker interface : http://$MASTER_PUBLIC_DNS:9100" >> $LOG_FILE
echo "Namenode interface : http://$MASTER_PUBLIC_DNS:9101" >> $LOG_FILE
echo "Master node: $MASTER_PUBLIC_DNS" >> $LOG_FILE

scp -o StrictHostKeyChecking=no -i $SSH_KEY -r $REPO_PATH/$TARGET_PATH/$JAR_FILE_NAME hadoop@$MASTER_PUBLIC_DNS:  >> $LOG_FILE 2>&1
#ssh -o StrictHostKeyChecking=no -i $SSH_KEY hadoop@$MASTER_PUBLIC_DNS "hadoop fs -rmr $S3_BUCKET_PATH/intermediate0" >> $LOG_FILE 2>&1
#ssh -o StrictHostKeyChecking=no -i $SSH_KEY hadoop@$MASTER_PUBLIC_DNS "hadoop fs -rmr $S3_BUCKET_PATH/output" >> $LOG_FILE 2>&1
ssh -o StrictHostKeyChecking=no -i $SSH_KEY hadoop@$MASTER_PUBLIC_DNS "hadoop jar recoengine-1.0-SNAPSHOT.jar com.xyz.reccommendation.driver.RecoEngineStage1 $S3_BUCKET_PATH/pview.csv $S3_BUCKET_PATH/intermediate0" >> $LOG_FILE 2>&1
ssh -o StrictHostKeyChecking=no -i $SSH_KEY hadoop@$MASTER_PUBLIC_DNS "hadoop jar recoengine-1.0-SNAPSHOT.jar com.xyz.reccommendation.join.RecoEngineStage2 $S3_BUCKET_PATH/intermediate0 $S3_BUCKET_PATH/sales_order_item_4_mnths.csv $S3_BUCKET_PATH/output" >> $LOG_FILE 2>&1

#terminate job 
#$EMR_DIR/elastic-mapreduce --jobflow $JOBFLOWID --terminate >> $LOG_FILE 2>&1

# Load data into DB
 
STOP=`date "+%Y-%m-%d %H:%M"`
echo $STOP >> $LOG_FILE
