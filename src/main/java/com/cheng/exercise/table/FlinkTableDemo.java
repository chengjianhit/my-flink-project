package com.cheng.exercise.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

public class FlinkTableDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        DataSet<String> stringDataSource = env.readTextFile("UserBehavior3.csv");
        stringDataSource.print();

        DataSet<PlayerData> dataSet = stringDataSource.map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String s) throws Exception {
                String[] split = s.split(",");
                return new PlayerData(String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        String.valueOf(split[2]),
                        Integer.valueOf(split[3]),
                        Double.valueOf(split[4]),
                        Double.valueOf(split[5]),
                        Double.valueOf(split[6]),
                        Double.valueOf(split[7]),
                        Double.valueOf(split[8])
                );
            }
        });

        Table scoreTable =  tableEnv.fromDataSet(dataSet);
        tableEnv.registerTable("score", scoreTable);

        Table sqlQuery = tableEnv.sqlQuery("select player, count(assists) as totalAsis,count(season) as num from score group by player order by num desc limit 3");

        DataSet<Result> resultDataSet =  tableEnv.toDataSet(sqlQuery, Result.class);
        resultDataSet.print();

        TableSink sink = new CsvTableSink("result/TEST1", ",");
        String[] fieldNames = {"player", "totalAsis","num"};
        TypeInformation[] fieldTypes = {Types.STRING, Types.LONG, Types.LONG};
        tableEnv.registerTableSink("TEST1", fieldNames, fieldTypes, sink);
        sqlQuery.insertInto("TEST1");
//        sqlQuery.writeToSink(sink);

//        env.setParallelism(1);
        env.execute("FlinkTable Demo");
    }


    public static class PlayerData {
        /**
         * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
         */
        public String season;
        public String player;
        public String play_num;
        public Integer first_court;
        public Double time;
        public Double assists;
        public Double steals;
        public Double blocks;
        public Double scores;

        public PlayerData() {
            super();
        }

        public PlayerData(String season,
                          String player,
                          String play_num,
                          Integer first_court,
                          Double time,
                          Double assists,
                          Double steals,
                          Double blocks,
                          Double scores
        ) {
            this.season = season;
            this.player = player;
            this.play_num = play_num;
            this.first_court = first_court;
            this.time = time;
            this.assists = assists;
            this.steals = steals;
            this.blocks = blocks;
            this.scores = scores;
        }
    }

    public static class Result {
        public String player;
        public Long num;
        public Long totalAsis;

        public Result() {
            super();
        }
        public Result(String player, Long num, Long totalAsis) {
            this.player = player;
            this.num = num;
            this.totalAsis = totalAsis;
        }
        @Override
        public String toString() {
            return player + ":" + num;
        }
    }
}
