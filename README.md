# clusterj-test
A test program for MySql Clusterj library.

# How to run
1. build the project using mvn clean install 
2. set LD_LIBRARY_PATH to your libndbclient.so
3. create the following table in your database
```sql
  CREATE TABLE `clusterj-test-inode` (
  `id` int(11) NOT NULL,
  `parent_id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(3000) NOT NULL DEFAULT '',
  `modification_time` bigint(20) DEFAULT NULL,
  `access_time` bigint(20) DEFAULT NULL,
  `size` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`parent_id`,`name`),
  KEY `inode_idx` (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
PARTITION BY KEY (parent_id)
```
4. run the following command
```bash
  ./clusterj-rc-test.sh MYSQL_CLUSTER_HOST DATA_BASE_NAME
```
5. check stats folder for the results, you should have gnuplot installed to get a graph

# Example
Setup: 6 datanode mysql cluster 7.4.4, the experiment ran on a machine with 32 cores.

![alt tag](https://github.com/maismail/clusterj-test/blob/master/cluster-rc-one-machine_32cores.png)
