package org.jasmine.stream.models;

import org.jasmine.stream.utils.JSONStringable;
import org.jasmine.stream.utils.Mergeable;

import java.util.HashMap;

public class CommentHourlyCount implements JSONStringable, Mergeable<CommentHourlyCount> {
    private long ts;
    private double count_h00; //00:00:00 a 01:59:59
    private double count_h02; //02:00:00 a 03:59:59
    private double count_h04; //04:00:00 a 05:59:59
    private double count_h06; //06:00:00 a 07:59:59
    private double count_h08; //08:00:00 a 09:59:59
    private double count_h10; //10:00:00 a 11:59:59
    private double count_h12; //12:00:00 a 13:59:59
    private double count_h14; //14:00:00 a 15:59:59
    private double count_h16; //16:00:00 a 17:59:59
    private double count_h18; //18:00:00 a 19:59:59
    private double count_h20; //20:00:00 a 21:59:59
    private double count_h22; //22:00:00 a 23:59:59

    public CommentHourlyCount(long ts, double count_h00, double count_h02, double count_h04, double count_h06, double count_h08, double count_h10, double count_h12, double count_h14, double count_h16, double count_h18, double count_h20, double count_h22) {
        this.ts = ts;
        this.count_h00 = count_h00;
        this.count_h02 = count_h02;
        this.count_h04 = count_h04;
        this.count_h06 = count_h06;
        this.count_h08 = count_h08;
        this.count_h10 = count_h10;
        this.count_h12 = count_h12;
        this.count_h14 = count_h14;
        this.count_h16 = count_h16;
        this.count_h18 = count_h18;
        this.count_h20 = count_h20;
        this.count_h22 = count_h22;
    }

    public CommentHourlyCount(long timestamp, HashMap<Integer, Long> hashMap) {
        this(timestamp, hashMap.getOrDefault(0, 0L), hashMap.getOrDefault(1, 0L), hashMap.getOrDefault(2, 0L), hashMap.getOrDefault(3, 0L), hashMap.getOrDefault(4, 0L), hashMap.getOrDefault(5, 0L), hashMap.getOrDefault(6, 0L), hashMap.getOrDefault(7, 0L), hashMap.getOrDefault(8, 0L), hashMap.getOrDefault(9, 0L), hashMap.getOrDefault(10, 0L), hashMap.getOrDefault(11, 0L));
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public double getCount_h00() {
        return count_h00;
    }

    public void setCount_h00(double count_h00) {
        this.count_h00 = count_h00;
    }

    public double getCount_h02() {
        return count_h02;
    }

    public void setCount_h02(double count_h02) {
        this.count_h02 = count_h02;
    }

    public double getCount_h04() {
        return count_h04;
    }

    public void setCount_h04(double count_h04) {
        this.count_h04 = count_h04;
    }

    public double getCount_h06() {
        return count_h06;
    }

    public void setCount_h06(double count_h06) {
        this.count_h06 = count_h06;
    }

    public double getCount_h08() {
        return count_h08;
    }

    public void setCount_h08(double count_h08) {
        this.count_h08 = count_h08;
    }

    public double getCount_h10() {
        return count_h10;
    }

    public void setCount_h10(double count_h10) {
        this.count_h10 = count_h10;
    }

    public double getCount_h12() {
        return count_h12;
    }

    public void setCount_h12(double count_h12) {
        this.count_h12 = count_h12;
    }

    public double getCount_h14() {
        return count_h14;
    }

    public void setCount_h14(double count_h14) {
        this.count_h14 = count_h14;
    }

    public double getCount_h16() {
        return count_h16;
    }

    public void setCount_h16(double count_h16) {
        this.count_h16 = count_h16;
    }

    public double getCount_h18() {
        return count_h18;
    }

    public void setCount_h18(double count_h18) {
        this.count_h18 = count_h18;
    }

    public double getCount_h20() {
        return count_h20;
    }

    public void setCount_h20(double count_h20) {
        this.count_h20 = count_h20;
    }

    public double getCount_h22() {
        return count_h22;
    }

    public void setCount_h22(double count_h22) {
        this.count_h22 = count_h22;
    }

    @Override
    public String toString() {
        return this.toJSONString();
    }

    public CommentHourlyCount merge(CommentHourlyCount other) {
        this.ts = Math.min(this.ts, other.ts);
        this.count_h00 += other.count_h00;
        this.count_h02 += other.count_h02;
        this.count_h04 += other.count_h04;
        this.count_h06 += other.count_h06;
        this.count_h08 += other.count_h08;
        this.count_h10 += other.count_h10;
        this.count_h12 += other.count_h12;
        this.count_h14 += other.count_h14;
        this.count_h16 += other.count_h16;
        this.count_h18 += other.count_h18;
        this.count_h20 += other.count_h20;
        this.count_h22 += other.count_h22;
        return this;
    }

    public CommentHourlyCount multiply(double multiplier) {
        this.count_h00 *= multiplier;
        this.count_h02 *= multiplier;
        this.count_h04 *= multiplier;
        this.count_h06 *= multiplier;
        this.count_h08 *= multiplier;
        this.count_h10 *= multiplier;
        this.count_h12 *= multiplier;
        this.count_h14 *= multiplier;
        this.count_h16 *= multiplier;
        this.count_h18 *= multiplier;
        this.count_h20 *= multiplier;
        this.count_h22 *= multiplier;
        return this;
    }
}
