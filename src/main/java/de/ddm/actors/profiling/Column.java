package de.ddm.actors.profiling;

import lombok.Getter;

import java.util.HashSet;

public class Column {
    private final int id;
    //To make sure that the values are unique
    private final HashSet<String> columnValues;
    private final String type;
    @Getter
    private String columnName;
    @Getter
    private String nameOfDataset;

    Column(int id, String type, String columnName, String nameOfDataset){
        this.id = id;
        this.type = type;
        this.columnName = columnName;
        this.nameOfDataset = nameOfDataset;
        columnValues = new HashSet<>();
    }
    void addValueToColumn(String value){
        columnValues.add(value);
    }

    int getId(){
        return id;
    }

    HashSet<String> getColumnValues(){
        return columnValues;
    }

    public String getType() {
    	return this.type;
    }

}
