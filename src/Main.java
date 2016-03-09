import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by laj on 8-3-2016.
 */
public class Main {

    // Per gebruiker aantal posts

    public static void main(String [ ] args) throws Exception {

        Logger LOG = LoggerFactory.getLogger(Main.class);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read text file from local files system
        DataSet<String> text = env.readTextFile("./res/out.edit-eswiki");

        /*
        DataSet<Tuple2<Integer, Integer>> postsPerUser = text
                .flatMap(new PostsPerUser())
                .groupBy(0)
                .sum(1);

        DataSet<Tuple2<Integer, Integer>> filteredPostPerUser = postsPerUser
                .flatMap(new FilterPostsPerUser());

*/
        DataSet<Tuple3<Integer, Long, Long>> timestampIntervalPerUser = text
                .flatMap(new TimestampIntervalPerUser());



        timestampIntervalPerUser.print();

    }

    public static class PostsPerUser implements FlatMapFunction<String, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<Integer, Integer>> out) {
            String[] linearguments = line.split(" ");

            try {
                int UserId = Integer.parseInt(linearguments[0]);
                int PageId = Integer.parseInt(linearguments[1]);
                long timestamp = Long.parseLong(linearguments[3]);

                out.collect(new Tuple2<Integer, Integer>(UserId, 1));
            } catch(Exception ex)
            {}
        }
    }

    public static class FilterPostsPerUser implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(Tuple2<Integer, Integer> tuple, Collector<Tuple2<Integer, Integer>> out) {
            if (tuple.f1 > 5) {
                out.collect(tuple);
            }
        }
    }

    public static class TimestampIntervalPerUser implements FlatMapFunction<String, Tuple3<Integer, Long, Long>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<Integer, Long, Long>> out) {
            String[] linearguments = line.split(" ");

            try {
                int UserId = Integer.parseInt(linearguments[0]);
                int PageId = Integer.parseInt(linearguments[1]);
                long timestamp = Long.parseLong(linearguments[3]);

                out.collect(new Tuple3<Integer, Long, Long>(UserId, timestamp, timestamp + 604800L));
            } catch(Exception ex)
            {}
        }
    }




}
