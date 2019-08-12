package com.cheng.exercise;

import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HotGoodsDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //按照EventTime处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

//        env.setStateBackend();
//        URL resource = HotGoodsDemo.class.getClassLoader().getResource("UserBehavior.csv");

//        Path filePath = Path.fromLocalFile(new File(resource.toURI()));

//        Path filePath = Path.fromLocalFile(new File("UserBehavior2.csv"));
//        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>)TypeExtractor.createTypeInfo(UserBehavior.class);
//        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
//        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

//        env
//                .createInput(csvInput, pojoType)
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
//                    @Override
//                    public long extractAscendingTimestamp(UserBehavior element) {
//                        return element.timestamp * 1000;
//                    }
//                })
//                .filter(new FilterFunction<UserBehavior>() {
//                    @Override
//                    public boolean filter(UserBehavior value) throws Exception {
//                        return value.behavior.equals("pv");
//                    }
//                })
//                .keyBy("itemId")
//                .timeWindow(Time.minutes(60), Time.minutes(5))
//                .aggregate(new CountAgg(), new WindowResultFunction())
//                .keyBy("windowEnd")
//                .process(new TopNHotItems(3))
//                .print();

        DataStream<UserBehavior> dataStream = env.readTextFile("UserBehavior2.csv").map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] splitArray = value.split(",");
                return new UserBehavior(Long.valueOf(splitArray[0]), Long.valueOf(splitArray[1]), Long.valueOf(splitArray[2]), splitArray[3], Long.valueOf(splitArray[4]));
            }
        });

        DataStream<UserBehavior> assignData = dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.timestamp * 1000;
            }
        });

        DataStream<UserBehavior> pvData = assignData.filter(element -> element.getBehavior().equals("pv") ? true : false);

        KeyedStream<UserBehavior, Tuple> keyedData = pvData.keyBy("itemId");

        WindowedStream<UserBehavior, Tuple, TimeWindow> windowedStream = keyedData.timeWindow(Time.minutes(60), Time.minutes(5));

        DataStream<ItemViewCount> aggregateData = windowedStream.aggregate(new CountAgg(), new WindowResultFunction());

        aggregateData.keyBy("windowEnd")
                .process(new TopNHotItems(3))
                .print();

        System.out.println(env.getExecutionPlan());
        env.execute("Cheng Hot Job");


    }

    /** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        private ListState<ItemViewCount> itemState;


        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd+1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item: itemState.get()) {
                allItems.add(item);
            }
            itemState.clear();

            //降序排序
            allItems.sort((o1, o2) -> (int) (o2.viewCount-o1.viewCount));

            StringBuilder sb = new StringBuilder();

            sb.append("=======================================================================\n");
            sb.append("时间：").append(new Timestamp(timestamp + 1)).append("\n");

            for (int i=0; i< topSize;i++){
                ItemViewCount itemViewCount = allItems.get(i);
                sb.append("No= ").append(i)
                        .append(" 商品Id= ").append(itemViewCount.itemId)
                        .append(" 浏览次数= ").append(itemViewCount.viewCount)
                .append("\n");
            }

            sb.append("=======================================================================\n \n");
            TimeUnit.MILLISECONDS.sleep(1000);
//            System.out.println(sb.toString());
            out.collect(sb.toString());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<ItemViewCount> stateDescriptor = new ListStateDescriptor<>(
                    "item-State",
                    ItemViewCount.class
            );

            itemState = getRuntimeContext().getListState(stateDescriptor);
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = ((Tuple1<Long>)tuple).f0;
            Long viewCount = input.iterator().next();
            out.collect(ItemViewCount.of(itemId, window.getEnd(), viewCount));
        }
    }


    public static class ItemViewCount{
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }
    }

    @Data
    public static class UserBehavior{
        //用户Id
        public long userId;
        //尚品Id
        public long itemId;
        //类目Id
        public long categoryId;
        //用户行为
        public String behavior;
        //时间戳
        public long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(long userId, long itemId, long categoryId, String behavior, long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }
    }
}
