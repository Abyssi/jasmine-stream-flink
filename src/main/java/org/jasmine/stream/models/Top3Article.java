package org.jasmine.stream.models;

import org.jasmine.stream.utils.JSONStringable;

public class Top3Article implements JSONStringable {
    private long ts;
    private String artID1;
    private long nCmnt1;
    private String artID2;
    private long nCmnt2;
    private String artID3;
    private long nCmnt3;

    public Top3Article() {
    }

    public Top3Article(long ts, String artID1, long nCmnt1, String artID2, long nCmnt2, String artID3, long nCmnt3) {
        this.ts = ts;
        this.artID1 = artID1;
        this.nCmnt1 = nCmnt1;
        this.artID2 = artID2;
        this.nCmnt2 = nCmnt2;
        this.artID3 = artID3;
        this.nCmnt3 = nCmnt3;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getArtID1() {
        return artID1;
    }

    public void setArtID1(String artID1) {
        this.artID1 = artID1;
    }

    public long getnCmnt1() {
        return nCmnt1;
    }

    public void setnCmnt1(long nCmnt1) {
        this.nCmnt1 = nCmnt1;
    }

    public String getArtID2() {
        return artID2;
    }

    public void setArtID2(String artID2) {
        this.artID2 = artID2;
    }

    public long getnCmnt2() {
        return nCmnt2;
    }

    public void setnCmnt2(long nCmnt2) {
        this.nCmnt2 = nCmnt2;
    }

    public String getArtID3() {
        return artID3;
    }

    public void setArtID3(String artID3) {
        this.artID3 = artID3;
    }

    public long getnCmnt3() {
        return nCmnt3;
    }

    public void setnCmnt3(long nCmnt3) {
        this.nCmnt3 = nCmnt3;
    }

    @Override
    public String toString() {
        return this.toJSONString();
    }
}
