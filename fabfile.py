from fabric.api import *
import fabric.contrib.files
import time
import logging
import os
from fabric.contrib.files import append

#Disable annoyting log messages.
logging.basicConfig(level=logging.ERROR)

#This makes the paramiko logger less verbose
para_log=logging.getLogger('paramiko.transport')
para_log.setLevel(logging.ERROR)

env.keepalive = True


########INPUT PARAMETERS###############
env.roledefs = {
    'masters': ['192.168.160.11'],
    'slaves': ['192.168.160.12', '192.168.160.13', '192.168.160.14', '192.168.160.15', '192.168.160.16', '192.168.160.17', '192.168.160.18', '192.168.160.19', '192.168.160.20', '192.168.160.21', '192.168.160.22'],
}

mastercluster = '192.168.160.11'


env.user='user'
#env.password='password'
#env.key_filename = '/home/dlequoc/.ssh/id_rsa'
home='/home/user/dlequoc/'
clusterhome = '/home/user/dlequoc/scone-spark/'
scalahome = clusterhome + 'scala'
sparkhome = '/home/user/dlequoc/scone-spark/spark'

urlHadoop = 'http://mirror.23media.de/apache/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz'
urlSpark = 'http://mirror.23media.de/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz'
urlScala = 'http://downloads.typesafe.com/scala/2.11.8/scala-2.11.8.tgz'
urlSbt = 'http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.7/sbt-launch.jar' 

maptask = 80
reducetask = 80



######INSTALL NUTCH HADOOP CASSANDRA#####

@roles('masters', 'slaves')
def installRequirement():
    sudo('sudo apt-get update')
    run('echo "yes"|sudo apt-get install ant')
    run('echo "yes"|sudo apt-get install openjdk-7-jdk')

@roles('masters', 'slaves')
def downloadHadoopSpark():
    run('rm -rf '+ clusterhome + ' && mkdir ' + clusterhome)

    run('cd ' + clusterhome + ' && wget ' + urlHadoop)
    tarhadoop = urlHadoop.split('/') [-1]
    hadoop = tarhadoop.split('.tar')[0]
    run('cd ' + clusterhome + ' && tar -xvzf ' + tarhadoop + ' && rm -rf hadoop && mv ' + hadoop + ' hadoop')

    run('cd ' + clusterhome + ' && wget ' + urlScala)
    tarscala = urlScala.split('/') [-1]
    scala = tarscala.split('.tgz')[0]
    scalahome = clusterhome + 'scala'
    run('cd ' + clusterhome + ' && tar -xvzf ' + tarscala + ' && rm -rf scala && mv ' + scala + ' scala')
    
    run('cd ' + clusterhome + ' && wget ' + urlSpark)
    tarspark = urlSpark.split('/') [-1]
    spark = tarspark.split('.tgz')[0]
    sparkhome = clusterhome + 'spark'
    run('cd ' + clusterhome + ' && tar -xvzf ' + tarspark + ' && rm -rf spark && mv ' + spark + ' spark')


@roles('masters', 'slaves')
def downloadSpark():
    sparkhome = clusterhome + 'spark'
    #run('mv '+ sparkhome + ' ' + sparkhome + '/../spark-old3' + ' && mkdir ' + sparkhome)

    run('cd ' + clusterhome + ' && wget ' + urlSpark)
    tarspark = urlSpark.split('/') [-1]
    spark = tarspark.split('.tgz')[0]
    sparkhome = clusterhome + 'spark'
    run('cd ' + clusterhome + ' && tar -xvzf ' + tarspark + ' && rm -rf spark && mv ' + spark + ' spark')


#####NUTCH CLUSTER MANAGEMENT#####
@roles('masters', 'slaves')
def cleanCluster():
    run('rm -rf ' + clusterhome + 'hadoop/logs/*')
    run('sudo rm -rf /tmp/*')
    run('rm -rf ' + clusterhome + 'hadoop/tmp/*')
    run('rm -rf ' + clusterhome + 'cassandra/logs/*')
    run('rm -rf ' + clusterhome + 'hadoop/hdfs/data/')
    run('rm -rf ' + clusterhome + 'hadoop/hdfs/name/')
    run('rm -rf ' + clusterhome + 'hadoop/hdfs/hadoop-unjar*')
    run('rm -rf ' + clusterhome + 'cassandra/var/')
    run('rm -rf ' + clusterhome + 'hadoop/hdfs/dfs/*')
    run('rm -rf ' + clusterhome + 'hadoop/hdfs/mapreduce/*')
    run('rm -rf ' + sparkhome + '/logs/*')


@roles('masters', 'slaves')
def removeHostKey():
    for host in env.roledefs['masters']:
        run('ssh-keygen -R ' + host)
    for host in env.roledefs['slaves']:
        run('ssh-keygen -R ' + host)   

@roles('masters', 'slaves')
def listHadooplogs():
    run('ls ' + clusterhome + 'hadoop/logs/')


@roles('masters')
def formatHadoop():
    with settings(warn_only=True):
        run('echo "Y\\n" |' + clusterhome + 'hadoop/bin/hadoop namenode -format')
        run(clusterhome + 'hadoop/bin/hadoop datanode -format')

def refreshCluster():
    execute(cleanCluster)
    execute(formatHadoop)



@roles('masters')
def startCluster():
    #if fabric.contrib.console.confirm("You tests failed do you want to continue?"):
    #run(clusterhome + 'hadoop/sbin/start-all.sh', pty=False) #start Hadoop
    run(sparkhome + '/sbin/start-all.sh', pty=False)

@roles('masters', 'slaves')
def stopCluster():
    run('pkill -9 java', pty=True)
    #run('sudo pkill -9 python', pty=True)

######HADOOP CONFIGURATION######
@roles('masters', 'slaves')
def changeMapRedSite(master=mastercluster, maptask=str(maptask), reducetask=str(reducetask)):
    filename = clusterhome + 'hadoop/etc/hadoop/mapred-site.xml.template'
    before = '<configuration>' #newfile is empty
    after = '<configuration>' + '\\n<property>\\n<name>mapred.job.tracker</name>\\n<value>' + master + ':9001</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.map.tasks</name>\\n<value>' + maptask + '</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.reduce.tasks</name>\\n<value>' + reducetask + '</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.system.dir</name>\\n<value>' + clusterhome + 'hadoop/hdfs/mapreduce/system</value>\\n</property>' + \
                                '\\n<property>\\n<name>mapred.local.dir</name>\\n<value>' + clusterhome + 'hadoop/hdfs/mapreduce/local</value>\\n</property>' 
    fabric.contrib.files.sed(filename, before, after, limit='')
    run('cd ' + clusterhome + 'hadoop/etc/hadoop/' + '&& mv mapred-site.xml.template mapred-site.xml')

@roles('masters', 'slaves')
def changeCoreSite(master=mastercluster):
    filename = clusterhome + 'hadoop/etc/hadoop/core-site.xml'
    before = '<configuration>' #newfile is empty
    after = '<configuration>' + '\\n<property>\\n<name>hadoop.tmp.dir</name>\\n<value>' + clusterhome + 'hadoop/hdfs</value>\\n</property>' + \
                                '\\n<property>\\n<name>fs.default.name</name>\\n<value>hdfs://' + master + ':9000</value>\\n</property>' 
    fabric.contrib.files.sed(filename, before, after, limit='')


@roles('masters', 'slaves')
def changeHDFSSite(master=mastercluster, replica='1', xcieversmax='10096'):
    filename = clusterhome + 'hadoop/etc/hadoop/hdfs-site.xml'
    before = '<configuration>' #newfile is empty
    after = '<configuration>' + '\\n<property>\\n<name>dfs.name.dir</name>\\n<value>' + clusterhome + 'hadoop/hdfs/name</value>\\n</property>' + \
                                '\\n<property>\\n<name>dfs.data.dir</name>\\n<value>' + clusterhome + 'hadoop/hdfs/data</value>\\n</property>' + \
                                '\\n<property>\\n<name>dfs.replication</name>\\n<value>' + replica + '</value>\\n</property>' + \
                                '\\n<property>\\n<name>dfs.datanode.max.xcievers</name>\\n<value>' + xcieversmax + '</value>\\n</property>'
    fabric.contrib.files.sed(filename, before, after, limit='')


@roles('masters', 'slaves')
def changeMasters(master=mastercluster):
    filename = clusterhome + 'hadoop/etc/hadoop/masters'
    before = 'localhost'
    after = master
    fabric.contrib.files.sed(filename, before, after, limit='')

@roles('masters', 'slaves')
def changeSlaves():
    filename = clusterhome + 'hadoop/etc/hadoop/slaves'
    before = 'localhost'
    after = ''
    slaves = env.roledefs['slaves']
    for slave in slaves:
        after = after + slave + '\\n' 
    fabric.contrib.files.sed(filename, before, after, limit='')


@roles('slaves')
def changeYarnSiteSlave(master=mastercluster):
    filename = clusterhome + 'hadoop/etc/hadoop/yarn-site.xml'
    before = '<configuration>' #newfile is empty
    after ='<configuration>' + '\\n<property>\\n<name>yarn.resourcemanager.hostname</name>\\n<value>' + master + '</value>\\n' + \
           '<description>The hostname of the ResourceManager</description>\\n' + \
           '</property>\\n' + \
           '<property>\\n<name>yarn.nodemanager.aux-services</name>\\n' + \
           '<value>mapreduce_shuffle</value>\\n</property>\\n'
    fabric.contrib.files.sed(filename, before, after, limit='')
               
 
@roles('masters')
def changeYarnSiteMaster(master=mastercluster):
    filename = clusterhome + 'hadoop/etc/hadoop/yarn-site.xml'
    before = '<configuration>' #newfile is empty
    after ='<configuration>' + '\\n<property>\\n<name>yarn.resourcemanager.hostname</name>\\n<value>' + master + '</value>\\n' + \
           '<description>The hostname of the ResourceManager</description>\\n' + \
           '</property>\\n' + \
           '<property>\\n<name>yarn.nodemanager.aux-services</name>\\n' + \
           '<value>mapreduce_shuffle</value>\\n</property>\\n' + \
           '<property><name>yarn.nodemanager.resource.memory-mb</name>\\n' + \
           '<value>2048</value>\\n </property>\\n' + \
           '<property>\\n<name>yarn.scheduler.minimum-allocation-mb</name>\\n' + \
           '<value>1024</value>\\n</property>\\n' + \
           '<property><name>yarn.scheduler.maximum-allocation-mb</name>\\n' + \
           '<value>2048</value>\\n</property>\\n' + \
           '<property>\\n<name>yarn.app.mapreduce.am.resource.mb</name>\\n' + \
           '<value>1024</value>\\n</property>\\n' + \
           '<property>\\n<name>yarn.app.mapreduce.am.command-opts</name>\\n' + \
           '<value>-Xmx1024M</value>\\n</property>\\n' 

    fabric.contrib.files.sed(filename, before, after, limit='')


@serial
def configHadoop():
    execute(changeMapRedSite)
    execute(changeCoreSite)
    execute(changeHDFSSite)
    execute(changeYarnSiteMaster)
    execute(changeYarnSiteSlave)
    #execute(changeMasters)
    execute(changeSlaves)
    
    
@roles('masters', 'slaves')
def changeLimitsUbuntu(): #require sudo permission
    cmd = 'echo "* hard nofile 128000" | sudo tee -a /etc/security/limits.conf && echo "* soft nofile 128000" | sudo tee -a /etc/security/limits.conf'
    sudo(cmd)


#Install Spark
@roles('masters', 'slaves')
def changeSparkSlaves():
    filename = sparkhome + '/conf/slaves.template'
    before = 'localhost'
    after = ''
    slaves = env.roledefs['slaves']
    for slave in slaves:
        after = after + slave + '\\n'
    fabric.contrib.files.sed(filename, before, after, limit='')
    run('mv ' + filename + ' ' + sparkhome +'/conf/slaves')


@roles('masters', 'slaves')
def changeBashrc():
    bashhome = home + ".bashrc"
    content = '\n#SPARK\n' + 'export SCALA_HOME=' + scalahome + '\n' + 'export PATH=$PATH:' + scalahome + '/bin\n' + \
              'export SPARK_HOME=' + sparkhome + '\n' + \
              'export PATH=$PATH:' + sparkhome +'/bin\n' + \
              'export YARN_CONF_DIR=' + clusterhome + 'hadoop/etc/hadoop\n' + \
              'export HADOOP_CONF_DIR='+ clusterhome + 'hadoop/etc/hadoop' 
    fabric.contrib.files.append(bashhome, content, use_sudo=False, partial=False, escape=True, shell=False)

@roles('masters', 'slaves')
def compileSpark():
    cmd = 'cd ' + sparkhome + '/build &&' + 'rm -rf *.jar && wget ' + urlSbt + ' && mv sbt-launch.jar sbt-launch-0.13.7.jar'
    run(cmd)
    cmd = 'cd ' + sparkhome + ' && build/sbt -Pyarn -Phadoop-2.4 -DskipTests assembly'
    run(cmd)

@serial
def configSpark():
    execute(changeSparkSlaves)


@roles('masters', 'slaves')


#####SETUP CLUSTER#####
@serial
def setupCluster():
    #execute(installRequirement)
    execute(downloadHadoopSpark)
    execute(configHadoop)
    execute(configSpark)


##### EXPERIMENTS #####
@serial
def runexperiment():
    execute(refreshCluster)
    execute(startCluster)
    execute(runSpark)
    execute(stopCluster)
