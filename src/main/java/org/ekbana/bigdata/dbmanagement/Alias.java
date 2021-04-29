package org.ekbana.bigdata.dbmanagement;

import javax.persistence.*;

@Entity
@Table(name="TABLE_ALIAS")
public class Alias {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    int table_id;
    private String name;
    private String alias;

    public Alias() {
    }

    public int getTable_id() {
        return table_id;
    }

    public void setTable_id(int table_id) {
        this.table_id = table_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return "Alias{" +
                "id=" + table_id +
                ", name='" + name + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }
}
