package lib.implement;

import lib.algorithm.EnumClass;

public class Customers {
    private String name;
    private EnumClass level;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EnumClass getLevel() {
        return level;
    }

    public void setLevel(EnumClass level) {
        this.level = level;
    }
}
