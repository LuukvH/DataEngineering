import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Vraag4 {
    public static void Solve() throws Exception {
        Logger LOG = LoggerFactory.getLogger(Main.class);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Path currentRelativePath = Paths.get("");
        String s = String.format("%s/output/vraag4/", currentRelativePath.toAbsolutePath().toString());

        // read text file from local files system
        DataSet<String> text = env.readTextFile("./res/out.edit-eswiki");

        DataSet<Tuple3<Integer, Integer, Integer>> weekEditCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0, 1)
                .sum(2)
                .filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
                        return value.f2 > 100 ;
                    }
                });

        DataSet<Tuple3<Integer, Integer, Integer>> minWeekEditCounts = weekEditCounts
                .flatMap(new weekSubtractor());

        DataSet<Tuple5<Integer, Integer, Integer, Integer, Integer>> combined =
                weekEditCounts.join(minWeekEditCounts)
                .where("f1").equalTo("f1")
                .projectFirst(1).projectFirst(0).projectFirst(2).projectSecond(0).projectSecond(2); // week pg1 pg1_count pg2 pg2_count

        combined.filter(new FilterFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                return value.f2 != value.f4 ;
            }
        });


        combined.writeAsCsv(String.format("file:///%s", s), FileSystem.WriteMode.OVERWRITE);

        // Output the execution plan
        FileUtils.writeStringToFile(new File(String.format("%splan.json", s)), env.getExecutionPlan());

        combined.collect();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple3<Integer, Integer, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<Integer, Integer, Integer>> out) {
            String[] linearguments = line.split(" ");

            try {
                int UserId = Integer.parseInt(linearguments[0]);
                int PageId = Integer.parseInt(linearguments[1]);
                long timestamp = Long.parseLong(linearguments[3]);

                int week = (int) Math.floorDiv(timestamp, 604800);
                out.collect(new Tuple3<Integer, Integer, Integer>(PageId, week, 1));
            } catch(Exception ex)
            {}
        }
    }

    public static class weekSubtractor implements FlatMapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {
        @Override
        public void flatMap(Tuple3<Integer, Integer, Integer> in, Collector<Tuple3<Integer, Integer, Integer>> out) {
                out.collect(new Tuple3<Integer, Integer, Integer>(in.f0, in.f1 - 1, in.f2));
        }
    }
}