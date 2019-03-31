# Clean log file
echo "" > log-memory.txt

# Get PID of python process
pid=`ps|grep python|awk -F " " '{print $1}'`

# Dump the  memory of the python process
/usr/bin/python dump-memory.py $pid  &> content-memory

# Extract secret in the memory
result=`cat content-memory | grep "Karate" | awk -F "." '{print $1}'`

# Add result to a log file
if [ -z "$result" ]; then 
    tail -n 10 content-memory &> log-memory.txt;
    cat log-memory.txt; 
else
    echo $result >> log-memory.txt;
    cat log-memory.txt;
fi
