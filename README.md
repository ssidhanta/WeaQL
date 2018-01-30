# thesis_code-master

Instructions to run this tool on Google cloud Platform:

Create docker images from the Docker files for EZK, Replicator, and Weaql nodes.
Create instances from the docker images as follows.
docker build -t gcr.io/danielanalytics-192314/ezkweaql .
docker push gcr.io/danielanalytics-192314/ezkweaql


Configure and run the EZK nodes as follows.
gcloud compute scp --recurse /Users/subhajitsidhanta/Dropbox/thesis_code-master root@ezkweaql:/home/subhajitsidhanta/ --zone europe-west1-b
gcloud compute --project "danielanalytics-192314" ssh --zone "europe-west1-b" "ezkweaql"
 sudo chmod -R 777 subhajitsidhanta/thesis_code-master
sudo apt-get install openjdk-8-jdk-headless
sudo java -Dlog4j.configuration=file:"/home/subhajitsidhanta/thesis_code-master/resources/log4j_zookeeper.properties" -jar dist/jars/zookeeper-server.jar resources/zoo.cfg

Configure and run the Replicator nodes as follows.
gcloud compute scp --recurse /Users/subhajitsidhanta/Dropbox/thesis_code-master root@replicator1:/home/subhajitsidhanta/ --zone europe-west1-b
gcloud compute --project "danielanalytics-192314" ssh --zone "europe-west1-b" "replicator1"
sudo apt-get install openjdk-8-jdk-headless
sudo  apt-get -y install mysql-server mysql-client
sudo cp thesis_code-master/my.cnf /etc/mysql/
sudo ./thesis_code-master/replicatorStart.sh 10.142.0.2
sudo ./thesis_code-master/serverStart.sh 10.142.0.2
java -jar thesis_code-master/dist/jars/db-transform.jar "10.132.0.5" "tpcc_crdt"
java -jar /home/subhajitsidhanta/thesis_code-master/dist/jars/tpcc-gendb.jar 10.132.0.5 "tpcc_crdt" 1
java -Dlog4j.configuration=file:"/home/subhajitsidhanta/thesis_code-master/resources/log4j_weakdb.properties" -jar /home/subhajitsidhanta/thesis_code-master/dist/jars/replicator.jar /home/subhajitsidhanta/thesis_code-master/resources/topologies/docker_tpcc_7node.xml /home/subhajitsidhanta/thesis_code-master/resources/environment/low_env_localhost_tpcc_default_coord.env 1

Configure and execute the Weaql nodes as follows.
gcloud compute scp --recurse /Users/subhajitsidhanta/Dropbox/thesis_code-master root@weaql1:/home/subhajitsidhanta/ --zone europe-west1-b
gcloud compute --project "danielanalytics-192314" ssh --zone "europe-west1-b" "weaql1"
sudo apt-get install openjdk-8-jdk-headless
sudo  apt-get -y install mysql-server mysql-client
sudo java "-Xms6G" "-Xmx10G" -Dlog4j.configuration=file:"/home/subhajitsidhanta/thesis_code-master/resources/log4j_weakdb.properties" -jar /home/subhajitsidhanta/thesis_code-master/dist/jars/tpcc-client.jar /home/subhajitsidhanta/thesis_code-master/resources/topologies/docker_tpcc_7node.xml /home/subhajitsidhanta/thesis_code-master/resources/environment/low_env_localhost_tpcc_default_coord.env /home/subhajitsidhanta/thesis_code-master/resources/tpcc/workload1 1 20 60 crdt
 find . -name '*.csv' -exec cat {} \;
