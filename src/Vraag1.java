import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by laj on 9-3-2016.
 */
public class Vraag1 {
    public static void Solve() throws Exception {
        Logger LOG = LoggerFactory.getLogger(Main.class);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read text file from local files system
        DataSet<String> text = env.readTextFile("./res/out.edit-eswiki");

        DataSet<Tuple2<Integer, Integer>> weekEditCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        weekEditCounts.writeAsCsv("file:///home/jeroen/Desktop/file.csv", FileSystem.WriteMode.OVERWRITE);
        weekEditCounts.collect();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<Integer, Integer>> out) {
            String[] linearguments = line.split(" ");

            try {
                int UserId = Integer.parseInt(linearguments[0]);
                int PageId = Integer.parseInt(linearguments[1]);
                long timestamp = Long.parseLong(linearguments[3]);

                int week = (int) Math.floorDiv(timestamp, 604800);

                out.collect(new Tuple2<Integer, Integer>(week, 1));
            } catch(Exception ex)
            {}
        }
    }
}
