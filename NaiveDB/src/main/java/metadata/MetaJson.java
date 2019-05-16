package metadata;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import util.CustomerException;

import java.io.File;
import java.util.Vector;

import metadata.TableInfo;

public class MetaJson {

    public String database;
    public String path;
    public String dbpath;
    public JSONObject singleMetaJsonObject;
    public TableInfo singletableInfo;

    public static final Integer TYPE_INT    = 3;
    public static final Integer TYPE_LONG   = 4;
    public static final Integer TYPE_FLOAT  = 5;
    public static final Integer TYPE_DOUBLE = 6;
    public static final Integer TYPE_STRING = 7;

    public MetaJson(String database){
        this.database = database;
        this.path = MetaData.DATABASES_DIR+"/"+this.database+"/"+MetaData.SINGLE_META_FILENAME;
        this.dbpath = MetaData.DATABASES_DIR + "/" + this.database;
        try{
            this.singleMetaJsonObject = new JSONObject(FileUtils.readFileToString(new File(this.path),"UTF-8"));
        } catch (Exception e){
            this.singleMetaJsonObject = new JSONObject("{}");
            System.out.println("could not load single_meta_json file and create one");
        }
    }

    public void writeBack(){
        try{
            FileUtils.writeStringToFile(new File(path), singleMetaJsonObject.toString(), "UTF-8");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getTableList(){
        return database + " : "+singleMetaJsonObject.keySet().toString();
    }

    public Boolean hasTable(String tableName){
        return singleMetaJsonObject.has(tableName);
    }

    public String createTable(String tableName, JSONObject tableInfo){
        if (! hasTable(tableName)){
            singleMetaJsonObject.put(tableName,tableInfo);
            writeBack();
            return String.format("have created table: %s",tableName);
        }
        return String.format("cannot create an already-existed table: %s",tableName);
    }

    public String dropTable(String tableName){
        if (hasTable(tableName)){
            String tablePath = getTablePath(tableName);
            if(tablePath != null){
                FileUtils.deleteQuietly(new File(tablePath));
            }
            singleMetaJsonObject.remove(tableName);
            writeBack();
            return String.format("have dropped table: %s",tableName);
        }
        return String.format("cannot drop a non-exist table: %s",tableName);
    }

    public String getTablePath(String tableName){
        if(hasTable(tableName)){
            return singleMetaJsonObject.getJSONObject(tableName).getString("filepath");
        }
        return null;
    }

    public Vector<String> getAttributesName(String tableName){
        if(hasTable(tableName)){
            JSONArray tmpJSONArray = singleMetaJsonObject.getJSONObject(tableName).getJSONArray("attrs");
            Vector<String> result = new Vector<String>();
            for(int i = 0; i < tmpJSONArray.length(); i++){
                result.add(tmpJSONArray.getString(i));
            }
            return result;
        }
        throw new CustomerException("table not exist");
    }

    public Vector<Integer> getAttributesType(String tableName){
        if(hasTable(tableName)){
            JSONArray tmpJSONArray = singleMetaJsonObject.getJSONObject(tableName).getJSONArray("types");
            Vector<Integer> result = new Vector<Integer>();
            for(int i = 0; i < tmpJSONArray.length(); i++){
                result.add(tmpJSONArray.getInt(i));
            }
            return result;
        }
        throw new CustomerException("table not exist");
    }

    public Vector<Boolean> getAttributesNotNull(String tableName){
        if(hasTable(tableName)){
            JSONArray tmpJSONArray = singleMetaJsonObject.getJSONObject(tableName).getJSONArray("notnull");
            Vector<Boolean> result = new Vector<Boolean>();
            for(int i = 0; i < tmpJSONArray.length(); i++){
                result.add(tmpJSONArray.getBoolean(i));
            }
            return result;
        }
        throw new CustomerException("table not exist");
    }

    public Vector<String> getAttributesPKName(String tableName){
        if(hasTable(tableName)){
            JSONArray tmpJSONArray = singleMetaJsonObject.getJSONObject(tableName).getJSONArray("pkattrs");
            Vector<String> result = new Vector<String>();
            for(int i = 0; i < tmpJSONArray.length(); i++){
                result.add(tmpJSONArray.getString(i));
            }
            return result;
        }
        throw new CustomerException("table not exist");
    }

    public Vector<Integer> getAttributesPKType(String tableName){
        if(hasTable(tableName)){
            JSONArray tmpJSONArray = singleMetaJsonObject.getJSONObject(tableName).getJSONArray("pktypes");
            Vector<Integer> result = new Vector<Integer>();
            for(int i = 0; i < tmpJSONArray.length(); i++){
                result.add(tmpJSONArray.getInt(i));
            }
            return result;
        }
        throw new CustomerException("table not exist");
    }

    public Vector<Integer> getAttributesOffset(String tableName){
        if(hasTable(tableName)){
            JSONArray tmpJSONArray = singleMetaJsonObject.getJSONObject(tableName).getJSONArray("offsets");
            Vector<Integer> result = new Vector<Integer>();
            for(int i = 0; i < tmpJSONArray.length(); i++){
                result.add(tmpJSONArray.getInt(i));
            }
        }
        throw new CustomerException("table not exist");
    }

}