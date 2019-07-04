package org.jasmine.stream.models;

import org.jasmine.stream.utils.JSONStringable;

public class CommentHourlyCount implements JSONStringable {
    private long ts;
    private long count_h00; //00:00:00 a 01:59:59
    private long count_h02; //02:00:00 a 03:59:59
    private long count_h04; //04:00:00 a 05:59:59
    private long count_h06; //06:00:00 a 07:59:59
    private long count_h08; //08:00:00 a 09:59:59
    private long count_h10; //10:00:00 a 11:59:59
    private long count_h12; //12:00:00 a 13:59:59
    private long count_h14; //14:00:00 a 15:59:59
    private long count_h16; //16:00:00 a 17:59:59
    private long count_h18; //18:00:00 a 19:59:59
    private long count_h20; //20:00:00 a 21:59:59
    private long count_h22; //22:00:00 a 23:59:59

    public CommentHourlyCount(long ts, long count_h00, long count_h02, long count_h04, long count_h06, long count_h08, long count_h10, long count_h12, long count_h14, long count_h16, long count_h18, long count_h20, long count_h22) {
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

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getCount_h00() {
        return count_h00;
    }

    public void setCount_h00(long count_h00) {
        this.count_h00 = count_h00;
    }

    public long getCount_h02() {
        return count_h02;
    }

    public void setCount_h02(long count_h02) {
        this.count_h02 = count_h02;
    }

    public long getCount_h04() {
        return count_h04;
    }

    public void setCount_h04(long count_h04) {
        this.count_h04 = count_h04;
    }

    public long getCount_h06() {
        return count_h06;
    }

    public void setCount_h06(long count_h06) {
        this.count_h06 = count_h06;
    }

    public long getCount_h08() {
        return count_h08;
    }

    public void setCount_h08(long count_h08) {
        this.count_h08 = count_h08;
    }

    public long getCount_h10() {
        return count_h10;
    }

    public void setCount_h10(long count_h10) {
        this.count_h10 = count_h10;
    }

    public long getCount_h12() {
        return count_h12;
    }

    public void setCount_h12(long count_h12) {
        this.count_h12 = count_h12;
    }

    public long getCount_h14() {
        return count_h14;
    }

    public void setCount_h14(long count_h14) {
        this.count_h14 = count_h14;
    }

    public long getCount_h16() {
        return count_h16;
    }

    public void setCount_h16(long count_h16) {
        this.count_h16 = count_h16;
    }

    public long getCount_h18() {
        return count_h18;
    }

    public void setCount_h18(long count_h18) {
        this.count_h18 = count_h18;
    }

    public long getCount_h20() {
        return count_h20;
    }

    public void setCount_h20(long count_h20) {
        this.count_h20 = count_h20;
    }

    public long getCount_h22() {
        return count_h22;
    }

    public void setCount_h22(long count_h22) {
        this.count_h22 = count_h22;
    }

    @Override
    public String toString() {
        return this.toJSONString();
    }
}
