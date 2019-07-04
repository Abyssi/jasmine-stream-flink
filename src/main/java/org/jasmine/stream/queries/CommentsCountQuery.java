package org.jasmine.stream.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jasmine.stream.models.CommentHourlyCount;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.CommentType;
import org.jasmine.stream.operators.CollectorAggregateFunction;
import org.jasmine.stream.operators.CounterAggregateFunction;
import org.jasmine.stream.operators.TimestampEnrichProcessAllWindowFunction;
import org.jasmine.stream.utils.DateUtils;

import java.util.Calendar;
import java.util.HashMap;

public class CommentsCountQuery {
    public static DataStream<CommentHourlyCount> run(DataStream<CommentInfo> inputStream, Time window) {
        return inputStream
                .filter(item -> item.getCommentType() == CommentType.COMMENT)
                .map(item -> {
                    Calendar calendar = DateUtils.parseCalendar(item.getCreateDate());
                    return (int) Math.floor(calendar.get(Calendar.HOUR_OF_DAY) / 2.0);
                })
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CounterAggregateFunction<>())
                .windowAll(TumblingEventTimeWindows.of(window))
                .aggregate(new CollectorAggregateFunction<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> {
                    HashMap<Integer, Long> value = item.getElement();
                    return new CommentHourlyCount(item.getTimestamp(), value.getOrDefault(0, 0L), value.getOrDefault(1, 0L), value.getOrDefault(2, 0L), value.getOrDefault(3, 0L), value.getOrDefault(4, 0L), value.getOrDefault(5, 0L), value.getOrDefault(6, 0L), value.getOrDefault(7, 0L), value.getOrDefault(8, 0L), value.getOrDefault(9, 0L), value.getOrDefault(10, 0L), value.getOrDefault(11, 0L));
                });
    }
}
