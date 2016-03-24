import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by laj on 22-3-2016.
 */
public class Vraag3 {

    public static void Solve() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final Logger LOG = LoggerFactory.getLogger(Main.class);

        // read text file from local files system
        DataSet<String> text = env.readTextFile("./res/out.edit-eswiki");

        // Split text line and return unique UserId and PageId
        DataSet<Tuple2<Integer, Integer>> uniqueUsers = text
                .flatMap(new LineSplitter())
                .distinct()
                .groupBy(1)
                .sum(2)
                .flatMap(new CountPerArticle())
                .filter(new FilterFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
                        return value.f1 > 5;
                    }
                });

        DataSet<Tuple2<Integer, Integer>> editsPerArticle = text
                .flatMap(new LineSplitter())
                .groupBy(1)
                .sum(2)
                .flatMap(new CountPerArticle());

        DataSet<Tuple3<Integer, Integer, Integer>> combined =
                editsPerArticle.join(uniqueUsers)
                        .where("f0").equalTo("f0")
                        .projectFirst(0).projectFirst(1).projectSecond(1);

        combined.writeAsCsv("file:///home/jeroen/Desktop/output/", FileSystem.WriteMode.OVERWRITE);
        combined.collect();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple3<Integer, Integer, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<Integer, Integer, Integer>> out) {
            String[] linearguments = line.split(" ");

            try {
                int UserId = Integer.parseInt(linearguments[0]);
                int PageId = Integer.parseInt(linearguments[1]);

                out.collect(new Tuple3<Integer, Integer, Integer>(UserId, PageId, 1));
            } catch(Exception ex) {

            }
        }
    }

    public static class CountPerArticle implements FlatMapFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(Tuple3<Integer, Integer, Integer> in, Collector<Tuple2<Integer, Integer>> out) {
            out.collect(new Tuple2<Integer, Integer>(in.f1, in.f2));
        }
    }
}
