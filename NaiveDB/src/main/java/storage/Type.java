package storage;
import java.util.Map;
import java.util.HashMap;

public class Type {

    public static final Integer TYPE_INT    = 3;
    public static final Integer TYPE_LONG   = 4;
    public static final Integer TYPE_FLOAT  = 5;
    public static final Integer TYPE_DOUBLE = 6;
    public static final Integer TYPE_STRING = 7;

    public static final Integer OFFSET_INT = 4;
    public static final Integer OFFSET_LONG = 8;
    public static final Integer OFFSET_FLOAT = 4;
    public static final Integer OFFSET_DOUBLE = 8;

    public static final Map <String,Integer> TYPE_MAP = new HashMap<String,Integer>(){{
        put("int", TYPE_INT);
        put("long", TYPE_LONG);
        put("float", TYPE_FLOAT);
        put("double", TYPE_DOUBLE);
        put("string", TYPE_STRING);
        put("INT", TYPE_INT);
        put("LONG", TYPE_LONG);
        put("FLOAT", TYPE_FLOAT);
        put("DOUBLE", TYPE_DOUBLE);
        put("STRING", TYPE_STRING);
    }};

    public static final Map<String, Integer> OFFSET_MAP = new HashMap<String, Integer>(){{
        put("int", OFFSET_INT);
        put("long", OFFSET_LONG);
        put("float", OFFSET_FLOAT);
        put("double", OFFSET_DOUBLE);
        put("INT", OFFSET_INT);
        put("LONG", OFFSET_LONG);
        put("FLOAT", OFFSET_FLOAT);
        put("DOUBLE", OFFSET_DOUBLE);
    }};
}
