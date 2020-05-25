package com.xh.bigdata.learnonelearn.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;


//@Data
//@AllArgsConstructor
//@NoArgsConstructor
//@Builder
public class User2 extends SpecificRecordBase implements SpecificRecord {
    private String name;
    private int favoriteNumber;
    private String favoriteColor;

    public User2() {
    }

    public User2(String name, int favoriteNumber, String favoriteColor) {
        this.name = name;
        this.favoriteNumber = favoriteNumber;
        this.favoriteColor = favoriteColor;
    }

    @Override
    public Schema getSchema() {
        return new Schema.Parser().parse("{\"namespace\": \"com.xh.bigdata.learnonelearn.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "     {\"name\": \"name\", \"type\": \"string\"},\n" +
        "     {\"name\": \"favoriteNumber\",  \"type\": [\"int\", \"null\"]},\n" +
        "     {\"name\": \"favoriteColor\", \"type\": [\"string\", \"null\"]}\n" +
        " ]\n" +
        "}\n");
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0: return name;
            case 1: return favoriteNumber;
            case 2: return favoriteColor;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0: name = (String) value;break;
            case 1: favoriteNumber = (int) value;break;
            case 2: favoriteColor = (String) value;break;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(int favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }
}
