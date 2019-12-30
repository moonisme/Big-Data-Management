import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;


// public class graph combines distance and path into a struct.

// JavaRDD<String> Node is used to get node information from the given file
// as we need to update the node table by map(find all possible paths) reduce(choose the shortest path)
// but a single map reduce is not enough, as it will only update the node table once.
// In a graph with N nodes, the worst case from node a to node b is the path contains all nodes in this graph,
// which means we need to update n times at most. Node will return an arraylist stores distinct nodes in the given file.

// JavaPairRDD<String, graph> Node_Graph is used to store the node table, in initial, set start_node distance to 0
// and all other nodes distance to infinite, all nodes path set to "".
// key: node value: graph<dist, path>

// JavaPairRDD<String, Tuple2<String, Integer>> Source_End_Dist is used to store information get from the file.
// key: node value: <adjacent_node, dist>

// Join Node_Graph and Source_End_Dist by key, then use flatMapToPair to create an iterable object becasue we want
// node table update once in a single for loop. JavaPairRDD<String, graph> step is the iterable RDD we just created.
// It is the mapper used to find all adjacent nodes for each node and update the dist and path for the node table.

// Then we compare the distance from one node to all it's adjacent nodes and choose the one with the shortest distance.
// This is the reduce process and we store result into Node_Graph. The map reduce process will keep doing until exiting the loop.

// a filter is used to filter out the distance from a start node to itself as it's always 0.
// I reverse the key and value in Node_Graph and use sortByKey() to make it in ascending order, then swap the key and value back.
// JavaPairRDD is changed to JavaRDD as we do not need those parentheses in the output.


public class assignment2 {

    public static class graph implements Serializable, Comparable<graph> {
        Integer Distance;
        String Path;

        public graph(Integer distance, String path) {
            this.Distance = distance;
            this.Path = path;
        }

        public graph() {
            this.Distance = Integer.MAX_VALUE;
            this.Path = "";
        }

        public Integer getDist() {
            return this.Distance;
        }

        public void setDist(Integer distance) {
            this.Distance = distance;
        }

        public String getPath() {
            return this.Path;
        }

        public void setPath(String path) {
            this.Path = path;
        }

        @Override
        public String toString() {
            if (this.getDist() != Integer.MAX_VALUE) {
            	StringBuilder sb = new StringBuilder();
            	sb.append(Distance).append(',').append(Path);
                return sb.toString();
            }
            else return "-1,";
        }

        @Override
        public int compareTo(graph a) {
              return this.getDist().compareTo(a.getDist());
        }
	
    }

        public static void main(String[] args) {

            SparkConf conf = new SparkConf()
                    .setAppName("assig2")
                    .setMaster("local");

            JavaSparkContext context = new JavaSparkContext(conf);
            String start_node = args[0];
            JavaRDD<String> file = context.textFile(args[1]);
            
            JavaRDD<String> Node = file.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String line) throws Exception {
                    String[] parts = line.split(",");
                    return Arrays.asList(parts[0]).iterator();
                }
            });
            
            JavaPairRDD<String, graph> Node_Graph = Node.mapToPair(new PairFunction<String, String, graph>() {
                @Override
                public Tuple2<String, graph> call(String node) throws Exception {
                    if (node.equals(start_node)) {
                        graph dis_path = new graph(0, start_node);
                        return new Tuple2<String, graph>(node, dis_path);
                    } 
                    else 
                    {
                        graph dis_path = new graph();
                        return new Tuple2<String, graph>(node, dis_path);
                    }
                }
            });

            Integer loop = Integer.parseInt(String.valueOf(Node.count()));

            JavaPairRDD<String, Tuple2<String, Integer>> Source_End_Dist = file.mapToPair(new PairFunction<String, String,
                    Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
                    String[] parts = line.split(",");
                    String source = parts[0];
                    String end = parts[1];
                    Integer dist = Integer.parseInt(parts[2]);
                    return new Tuple2<String, Tuple2<String, Integer>>(source, new Tuple2<>(end, dist));
                }
            });

            for (int i = 0; i < loop; i++) {
                JavaPairRDD<String, Tuple2<graph, Tuple2<String, Integer>>> temp = Node_Graph.join(Source_End_Dist);             
                JavaPairRDD<String, graph> step = temp.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<graph, Tuple2<String, Integer>>>, String, graph>() {
                    @Override
                    public Iterator<Tuple2<String, graph>> call(Tuple2<String, Tuple2<graph, Tuple2<String, Integer>>> graph) throws Exception {
                        if (graph._2._1.getDist() == Integer.MAX_VALUE) {
                            return Arrays.asList(new Tuple2<>(graph._1, graph._2._1)).iterator();                    
                        } 
                        else 
                        {
                        	graph traversal = new graph();
                            traversal.setDist(graph._2._1.getDist() + graph._2._2._2);
                            traversal.setPath(graph._2._1.getPath() + "-" + graph._2._2._1);
                            return Arrays.asList(new Tuple2<>(graph._2._2._1, traversal),new Tuple2<>(graph._1, graph._2._1)).iterator();
                        }

                    }
                });

                Node_Graph = step.reduceByKey(new Function2<graph, graph, graph>() {
                    @Override
                    public graph call(graph a, graph b) throws Exception {
                        if (a.compareTo(b) > 0) {
                            return b;
                        } 
                        else return a;
                    }
                });
            }

          Node_Graph.filter(x -> !x._1.equals(start_node))
          			.mapToPair(x -> x.swap())
          			.sortByKey()
          			.mapToPair(x -> x.swap())
          			.map(x -> x._1 + "," + x._2)
          			.saveAsTextFile(args[2]);              
    }
}
