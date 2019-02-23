pkill -9 node
nohup log.io-harvester &> /var/log/log.io.txt &
nohup log.io-server &> /var/log/log.io.txt &
