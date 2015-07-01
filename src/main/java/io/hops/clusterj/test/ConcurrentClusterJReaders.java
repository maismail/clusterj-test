package io.hops.clusterj.test;

import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentClusterJReaders {

  @Option(name = "-totalTx", usage = "Total Transactions. Default 500")
  private int totalTx = 500;
  @Option(name = "-numThreads", usage = "Total Transactions. Default 1")
  private int numThreads = 1;
  @Option(name = "-lockMode", usage = "Lock Mode. (R, RC, W). Default is RC")
  private String lockModeStr = "RC";
  private LockMode lockMode = LockMode.READ_COMMITTED;
  @Option(name = "-schema", usage = "DB schemma name.")
  static private String schema = null;
  @Option(name = "-dbHost",
      usage = "com.mysql.clusterj.connectstring.")
  static private String dbHost = null;

  @Option(name = "-clean", usage = "Delete tables")
  private boolean clean = false;

  @Option(name = "-threadOffset", usage = "thread offset")
  private int threadOffset = 0;

  @Option(name = "-statsFile", usage = "stats file")
  private String statsFile;

  private AtomicInteger opsCompleted = new AtomicInteger(0);
  private SessionFactory dbSessionProvider;
  private List workers;

  ExecutorService executor = null;
  @Option(name = "-inodesPerTx",
      usage = "Max inodes to read/write in a tx. Default is 5")
  private static int INODES_PER_WORKER = 5;

  private DescriptiveStatistics elapsedTime = new DescriptiveStatistics();

  public void startApplication(String[] args)
      throws InterruptedException, IOException {
    parseArgs(args);
    setUP();
    startWorkers();
    executor = null;
  }

  private void parseArgs(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
      if (lockModeStr.compareToIgnoreCase("W") == 0) {
        lockMode = LockMode.EXCLUSIVE;
      } else if (lockModeStr.compareToIgnoreCase("R") == 0) {
        lockMode = LockMode.SHARED;
      } else if (lockModeStr.compareToIgnoreCase("RC") == 0) {
        lockMode = LockMode.READ_COMMITTED;
      }

    } catch (Exception e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println();
      System.exit(-1);
    }
  }

  public void setUP() {
    Properties props = new Properties();
    props.setProperty("com.mysql.clusterj.connectstring", dbHost);
    props.setProperty("com.mysql.clusterj.database", schema);
    props.setProperty("com.mysql.clusterj.connect.retries", "4");
    props.setProperty("com.mysql.clusterj.connect.delay", "5");
    props.setProperty("com.mysql.clusterj.connect.verbose", "1");
    props.setProperty("com.mysql.clusterj.connect.timeout.before", "30");
    props.setProperty("com.mysql.clusterj.connect.timeout.after", "20");
    props.setProperty("com.mysql.clusterj.max.transactions", "1024");
    props.setProperty("com.mysql.clusterj.connection.pool.size", "1");
    dbSessionProvider = ClusterJHelper.getSessionFactory(props);

    if (clean) {
      Session session = dbSessionProvider.getSession();
      ClusterJTestTable.deleteAll(session);
      session.close();
      System.exit(0);
    }

    workers = new ArrayList(numThreads);
    executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      DBWriter worker = new DBWriter(threadOffset + i, dbSessionProvider.getSession());
      workers.add(worker);
    }
    populateDB();
  }

  private void populateDB() {
    Session session = dbSessionProvider.getSession();
    for (Object worker : workers) {
      session.currentTransaction().begin();
      session.setLockMode(lockMode);
      ClusterJTestTable.updateINodes(((DBWriter)worker).id, session,
          INODES_PER_WORKER);
      session.currentTransaction().commit();
    }
    session.close();
  }

  public void startWorkers() throws InterruptedException, IOException {

    long startTime = System.currentTimeMillis();
    executor.invokeAll(workers);
    executor.shutdown();

    while (!executor.isTerminated()) {
      Thread.sleep(100);
    }
    long endTime = System.currentTimeMillis();

    System.out.println(numThreads + " " + elapsedTime.getN() + " " +
        elapsedTime.getMean() + " " + elapsedTime.getMin() + " " +
        elapsedTime.getMax());

    if(statsFile != null) {
      BufferedWriter writer =
          new BufferedWriter(new FileWriter(new File(statsFile)));
      writer.write("#" + (endTime - startTime));
      for (double val : elapsedTime.getValues()) {
        writer.write(String.valueOf(val));
        writer.newLine();
      }
      writer.close();
    }
    elapsedTime.clear();
  }

  public class DBWriter implements Callable {

    private final int id;
    private final Session dbSession;

    public DBWriter(int id, Session session) {
      this.id = id;
      this.dbSession = session;
    }

    @Override
    public Object call() {
      while (true) {
        try {
          long t1 = System.currentTimeMillis();
          dbSession.currentTransaction().begin();
          dbSession.setLockMode(lockMode);
          //read data
          ClusterJTestTable.readINodes(id, dbSession, INODES_PER_WORKER);
          dbSession.currentTransaction().commit();
          long elapsed = System.currentTimeMillis() - t1;
          elapsedTime.addValue(elapsed);

          if (opsCompleted.incrementAndGet() >= totalTx) {
            dbSession.close();
            return null;
          }

        } catch (Throwable e) {
          opsCompleted.incrementAndGet();
          e.printStackTrace();
          dbSession.currentTransaction().rollback();
        }
      }
    }

  }

  public static void main(String[] args)
      throws IOException, InterruptedException {
    new ConcurrentClusterJReaders().startApplication(args);
  }

}
