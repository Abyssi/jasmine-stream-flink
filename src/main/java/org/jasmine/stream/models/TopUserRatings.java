package org.jasmine.stream.models;

import org.apache.flink.api.java.tuple.Tuple2;
import org.jasmine.stream.utils.JSONStringable;

public class TopUserRatings implements JSONStringable {
    private long ts;
    private long user_1;
    private double rating_1;
    private long user_2;
    private double rating_2;
    private long user_3;
    private double rating_3;
    private long user_4;
    private double rating_4;
    private long user_5;
    private double rating_5;
    private long user_6;
    private double rating_6;
    private long user_7;
    private double rating_7;
    private long user_8;
    private double rating_8;
    private long user_9;
    private double rating_9;
    private long user_10;
    private double rating_10;

    public TopUserRatings(long ts, long user_1, double rating_1, long user_2, double rating_2, long user_3, double rating_3, long user_4, double rating_4, long user_5, double rating_5, long user_6, double rating_6, long user_7, double rating_7, long user_8, double rating_8, long user_9, double rating_9, long user_10, double rating_10) {
        this.ts = ts;
        this.user_1 = user_1;
        this.rating_1 = rating_1;
        this.user_2 = user_2;
        this.rating_2 = rating_2;
        this.user_3 = user_3;
        this.rating_3 = rating_3;
        this.user_4 = user_4;
        this.rating_4 = rating_4;
        this.user_5 = user_5;
        this.rating_5 = rating_5;
        this.user_6 = user_6;
        this.rating_6 = rating_6;
        this.user_7 = user_7;
        this.rating_7 = rating_7;
        this.user_8 = user_8;
        this.rating_8 = rating_8;
        this.user_9 = user_9;
        this.rating_9 = rating_9;
        this.user_10 = user_10;
        this.rating_10 = rating_10;
    }

    public TopUserRatings(long timestamp, Tuple2<Long, Double>[] array) {
        this(timestamp, array.length > 0 ? array[0].f0 : 0, array.length > 0 ? array[0].f1 : 0, array.length > 1 ? array[1].f0 : 0, array.length > 1 ? array[1].f1 : 0, array.length > 2 ? array[2].f0 : 0, array.length > 2 ? array[2].f1 : 0, array.length > 3 ? array[3].f0 : 0, array.length > 3 ? array[3].f1 : 0, array.length > 4 ? array[4].f0 : 0, array.length > 4 ? array[4].f1 : 0, array.length > 5 ? array[5].f0 : 0, array.length > 5 ? array[5].f1 : 0, array.length > 6 ? array[6].f0 : 0, array.length > 6 ? array[6].f1 : 0, array.length > 7 ? array[7].f0 : 0, array.length > 7 ? array[7].f1 : 0, array.length > 8 ? array[8].f0 : 0, array.length > 8 ? array[8].f1 : 0, array.length > 9 ? array[9].f0 : 0, array.length > 9 ? array[9].f1 : 0);
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getUser_1() {
        return user_1;
    }

    public void setUser_1(long user_1) {
        this.user_1 = user_1;
    }

    public double getRating_1() {
        return rating_1;
    }

    public void setRating_1(double rating_1) {
        this.rating_1 = rating_1;
    }

    public long getUser_2() {
        return user_2;
    }

    public void setUser_2(long user_2) {
        this.user_2 = user_2;
    }

    public double getRating_2() {
        return rating_2;
    }

    public void setRating_2(double rating_2) {
        this.rating_2 = rating_2;
    }

    public long getUser_3() {
        return user_3;
    }

    public void setUser_3(long user_3) {
        this.user_3 = user_3;
    }

    public double getRating_3() {
        return rating_3;
    }

    public void setRating_3(double rating_3) {
        this.rating_3 = rating_3;
    }

    public long getUser_4() {
        return user_4;
    }

    public void setUser_4(long user_4) {
        this.user_4 = user_4;
    }

    public double getRating_4() {
        return rating_4;
    }

    public void setRating_4(double rating_4) {
        this.rating_4 = rating_4;
    }

    public long getUser_5() {
        return user_5;
    }

    public void setUser_5(long user_5) {
        this.user_5 = user_5;
    }

    public double getRating_5() {
        return rating_5;
    }

    public void setRating_5(double rating_5) {
        this.rating_5 = rating_5;
    }

    public long getUser_6() {
        return user_6;
    }

    public void setUser_6(long user_6) {
        this.user_6 = user_6;
    }

    public double getRating_6() {
        return rating_6;
    }

    public void setRating_6(double rating_6) {
        this.rating_6 = rating_6;
    }

    public long getUser_7() {
        return user_7;
    }

    public void setUser_7(long user_7) {
        this.user_7 = user_7;
    }

    public double getRating_7() {
        return rating_7;
    }

    public void setRating_7(double rating_7) {
        this.rating_7 = rating_7;
    }

    public long getUser_8() {
        return user_8;
    }

    public void setUser_8(long user_8) {
        this.user_8 = user_8;
    }

    public double getRating_8() {
        return rating_8;
    }

    public void setRating_8(double rating_8) {
        this.rating_8 = rating_8;
    }

    public long getUser_9() {
        return user_9;
    }

    public void setUser_9(long user_9) {
        this.user_9 = user_9;
    }

    public double getRating_9() {
        return rating_9;
    }

    public void setRating_9(double rating_9) {
        this.rating_9 = rating_9;
    }

    public long getUser_10() {
        return user_10;
    }

    public void setUser_10(long user_10) {
        this.user_10 = user_10;
    }

    public double getRating_10() {
        return rating_10;
    }

    public void setRating_10(double rating_10) {
        this.rating_10 = rating_10;
    }

    @Override
    public String toString() {
        return this.toJSONString();
    }
}
