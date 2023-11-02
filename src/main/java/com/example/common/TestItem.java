package com.example.common;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

public class TestItem {

    String id;
    String mypk;
    OffsetDateTime offsetDateTime;
    String offsetDateTimeString;
    ZonedDateTime zonedDateTime;
    String zonedDateTimeString;
    String dateTimeStringWithTrailingZeros;
    public TestItem(String id, String mypk) {
        this.id = id;
        this.mypk = mypk;
    }

    public TestItem() {
    }

    public String getId() {
        return id;
    }

    public String getMypk() {
        return mypk;
    }

    public OffsetDateTime getOffsetDateTime() {
        return offsetDateTime;
    }

    public void setOffsetDateTime(OffsetDateTime offsetDateTime) {
        this.offsetDateTime = offsetDateTime;
    }

    public String getOffsetDateTimeString() {
        return offsetDateTimeString;
    }

    public void setOffsetDateTimeString(String offsetDateTimeString) {
        this.offsetDateTimeString = offsetDateTimeString;
    }

    public ZonedDateTime getZonedDateTime() {
        return zonedDateTime;
    }

    public void setZonedDateTime(ZonedDateTime zonedDateTime) {
        this.zonedDateTime = zonedDateTime;
    }

    public String getZonedDateTimeString() {
        return zonedDateTimeString;
    }

    public void setZonedDateTimeString(String zonedDateTimeString) {
        this.zonedDateTimeString = zonedDateTimeString;
    }

    public String getDateTimeStringWithTrailingZeros() {
        return dateTimeStringWithTrailingZeros;
    }

    public void setDateTimeStringWithTrailingZeros(String dateTimeStringWithTrailingZeros) {
        this.dateTimeStringWithTrailingZeros = dateTimeStringWithTrailingZeros;
    }

    @Override
    public String toString() {
        return "TestItem{" +
            "id='" + id + '\'' +
            ", mypk='" + mypk + '\'' +
            ", offsetDateTime=" + offsetDateTime +
            ", offsetDateTimeString='" + offsetDateTimeString + '\'' +
            ", zonedDateTime=" + zonedDateTime +
            ", zonedDateTimeString='" + zonedDateTimeString + '\'' +
            ", dateTimeStringWithTrailingZeros='" + dateTimeStringWithTrailingZeros + '\'' +
            '}';
    }
}
