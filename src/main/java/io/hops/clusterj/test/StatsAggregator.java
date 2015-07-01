package io.hops.clusterj.test;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class StatsAggregator {

  public static void main(String[] args) throws IOException {
    combineResults(new File(args[0]), Integer.valueOf(args[1]), Integer
        .valueOf(args[2]), Integer.valueOf(args[3]), Integer.valueOf(args[4]));
  }

  public static void combineResults(File statsDir, int START, int END, int
      TH_INC, int TH_END)
      throws IOException {

    for (int split = TH_INC; split <= TH_END; split += TH_INC) {

      BufferedWriter writer = new BufferedWriter(new FileWriter(new File
          (statsDir, "cluster-rc-split-" + split + ".dat")));
      writer.write("#numProcesses N Avg Min Max");
      writer.newLine();

      for (int p = START; p <= END; p++) {

        DescriptiveStatistics statistics = new DescriptiveStatistics();

        for (int i = p; i > 0; i--) {
          File thStatsDir = new File(statsDir, String.valueOf(p));
          String file = split + "-" + p + "-" + i;
          System.out.println("Read Data for " + file);
          try {
            BufferedReader reader = new BufferedReader(new FileReader(new File
                (thStatsDir, file)));
            String line;
            while ((line = reader.readLine()) != null) {
              if (!line.startsWith("#")) {
                statistics.addValue(Double.valueOf(line.trim()));
              }
            }
            reader.close();
          } catch (FileNotFoundException ex) {
            System.out.println("ERROR " + file + " NotFound");
          }
        }

        writer.write(p + " " + statistics.getN() + " " + statistics
            .getMean() + " " + statistics.getMin() + " " + statistics.getMax());
        writer.newLine();
        statistics.clear();
      }

      writer.close();
    }

    generateGnuPlot(statsDir, TH_INC, TH_END);
  }

  private static void generateGnuPlot(File statsDir, int TH_INC, int TH_END)
      throws IOException {
    String graph = "set terminal png enhanced\n" +
        "set style data lines\n" +
        "set border 3\n" +
        "set key outside top center\n" +
        "set grid\n" +
        "set output 'cluster-rc-one-machine.png'\n" +
        "set xlabel \"Number of Processes\"\n" +
        "set ylabel \"Time (miliseconds)\"\n" +
        "plot ";

    for (int split = TH_INC; split <= TH_END; split+=TH_INC) {
      graph += " \"cluster-rc-split-" + split + ".dat\" using 3:xtic(1) " +
          "title \" " + split + " Threads/Process\",";
    }

    BufferedWriter writer = new BufferedWriter(new FileWriter(new File
        (statsDir, "cluster-rc-one-machine.gnu")));
    writer.write(graph.substring(0, graph.length()-1));
    writer.close();
  }
}
