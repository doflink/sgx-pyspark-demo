# SGX-PySpark Demo

This repository contains SGX-PySpark demo.

## Video

[SGX-PySpark demo](https://youtu.be/yI3iEFWUWbU)

## Details

Try it out by executing:

```bash
git clone https://github.com/doflink/sgx-pyspark-demo && cd sgx-pyspark-demo 
```

```bash
docker run -it --rm -v `pwd`:/fspf  --privileged -p 8080:8080 -p 6868:6868 -p 28778:28778 lequocdo/google-c3:pyspark sh
```

Go to the demo directory:

```bash
 cd /fspf/
```

Create a file system protection file (meta file) to store all the metadata required for checking the consistency of files. 
Then add encrypted regions and encrypt the input PySpark codes and data. Store the encryption key and the tag of the fspf to the file keytag:

```bash
 ./fspf.sh
```

Export environment variables

```bash
export SCONE_FSPF_KEY=$(cat input/keytag | awk '{print $11}')
export SCONE_FSPF_TAG=$(cat input/keytag | awk '{print $9}')
```

Now, run the wordcount application with SGX-PySpark.

```bash
/spark/bin/spark-submit encrypted-files/enc-wordcount.py  input/encrypted-sensitive-input.txt  spark://$(hostname -f):7077 &> output.txt &
```

Try to dump memory of the application to steal secrets

```bash
./memory-dump.sh
```

## Contacts

Send email to lequocdo@gmail.com or do.le_quoc@tu-dresden.de
