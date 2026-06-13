package lib.algorithm;

public enum EnumClass {
    Standard("Standard Level"),
    Premium("Premium Level"),
    VIP("VIP Level");

    private final String description;

    EnumClass(String _description) {
        description = _description;
    }

    public String getDesc() {
        return description;
    }
}