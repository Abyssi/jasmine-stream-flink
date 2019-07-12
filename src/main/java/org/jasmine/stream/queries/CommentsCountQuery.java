package org.jasmine.stream.queries;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jasmine.stream.models.CommentHourlyCount;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.CommentType;
import org.jasmine.stream.operators.*;
import org.jasmine.stream.utils.DateUtils;

import java.util.Calendar;

public class CommentsCountQuery {
    @SuppressWarnings("Duplicates")
    public static DataStream<CommentHourlyCount> run(DataStream<CommentInfo> inputStream, Time window) {
        return inputStream
                .filter(item -> item.getCommentType() == CommentType.COMMENT)
                .map(item -> (int) Math.floor(DateUtils.parseCalendar(item.getCreateDate()).get(Calendar.HOUR_OF_DAY) / 2.0))
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CounterAggregateFunction<>())
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CollectorAggregateFunction<>())
                .windowAll(TumblingEventTimeWindows.of(window))
                .reduce(new CollectorAggregateFunction.Merge<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new CommentHourlyCount(item.getTimestamp(), item.getElement()));
    }

    @SuppressWarnings("Duplicates")
    public static Tuple3<DataStream<CommentHourlyCount>, DataStream<CommentHourlyCount>, DataStream<CommentHourlyCount>> runAll(DataStream<CommentInfo> inputStream) {
        Time window24h = Time.hours(24);
        Time window7d = Time.days(7);
        Time window1M = Time.days(30);

        DataStream<CommentHourlyCount> window24hStream = inputStream
                .filter(item -> item.getCommentType() == CommentType.COMMENT)
                .map(item -> (int) Math.floor(DateUtils.parseCalendar(item.getCreateDate()).get(Calendar.HOUR_OF_DAY) / 2.0))
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window24h))
                .aggregate(new CounterAggregateFunction<>())
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window24h))
                .aggregate(new CollectorAggregateFunction<>())
                .windowAll(TumblingEventTimeWindows.of(window24h))
                .reduce(new CollectorAggregateFunction.Merge<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new CommentHourlyCount(item.getTimestamp(), item.getElement()));

        DataStream<CommentHourlyCount> window7dStream = window24hStream
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window7d))
                .aggregate(new IdentifiedCommentHourlyCountAggregateFunction())
                .windowAll(TumblingEventTimeWindows.of(window7d))
                .reduce(CommentHourlyCount::merge);

        DataStream<CommentHourlyCount> window1MStream = window7dStream
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window1M))
                .aggregate(new IdentifiedCommentHourlyCountAggregateFunction())
                .windowAll(TumblingEventTimeWindows.of(window1M))
                .reduce(CommentHourlyCount::merge);

        return new Tuple3<>(window24hStream, window7dStream, window1MStream);
    }

    private static class IdentifiedCommentHourlyCountAggregateFunction extends IdentifiedMergeableAggregateFunction<CommentHourlyCount> {
    }
}
