package io.hops.clusterj.test;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ClusterJTestTable {
   /*
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
  */

  private static Random rand = new Random(System.currentTimeMillis());

  @PersistenceCapable(table = "clusterj-test-inode")
  @PartitionKey(column = "parent_id")
  @Index(name = "inode_idx")
  public static interface ClusterJTestINodeDTO {
    @Column(name = "id")
    int getId();
    void setId(int var1);

    @PrimaryKey
    @Column(name = "name")
    String getName();

    void setName(String var1);

    @PrimaryKey
    @Column(name = "parent_id")
    int getParentId();

    void setParentId(int var1);

    @Column(name = "modification_time")
    long getModificationTime();
    void setModificationTime(long var1);

    @Column(name = "access_time")
    long getATime();
    void setATime(long var1);
    @Column(name = "size")
    int getSize();
    void setSize(int var1);
  }



  public static void updateINodes(int workerId, Session session, int INODES_PER_WORKER) {
    for (int i = 0; i < INODES_PER_WORKER; i++) {
      ClusterJTestINodeDTO dto = session.newInstance(ClusterJTestINodeDTO.class);
      //setting unique pk
      int inodeId = (workerId * INODES_PER_WORKER) + i;
      dto.setParentId(inodeId);
      dto.setName("worker" + workerId + "-" + inodeId);
      dto.setId(inodeId);
      dto.setModificationTime(System.currentTimeMillis());
      dto.setATime(System.currentTimeMillis());
      dto.setSize(rand.nextInt());
      session.makePersistent(dto);
    }
  }

  public static List<ClusterJTestINodeDTO> readINodes(int workerId, Session
      session, int INODES_PER_WORKER) {
    List<ClusterJTestINodeDTO> readINodes = new ArrayList<ClusterJTestINodeDTO>(INODES_PER_WORKER);
    for (int i = 0; i < INODES_PER_WORKER; i++) {
      Object[] pk = new Object[2];
      pk[0] = (workerId * INODES_PER_WORKER) + i;
      pk[1] = "worker" + workerId + "-" + pk[0];
      ClusterJTestINodeDTO result = session.find(ClusterJTestINodeDTO.class, pk);
      if (result != null) {
        readINodes.add(result);
      }
    }
    if (readINodes.size() != INODES_PER_WORKER) {
      throw new IllegalStateException("Wrong number of INodes read");
    }
    return readINodes;
  }

  public static void deleteAll(Session session){
    session.deletePersistentAll(ClusterJTestINodeDTO.class);
  }

}
