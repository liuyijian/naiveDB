package metadata;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import storage.Type;
import util.CustomerException;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

import query.Query;
import storage.Storage;

import metadata.TableInfo;

public class MetaJson {

    public String database;
    // 元文件路径 = 数据库路径 + 元文件文件名
    public String path;
    // 数据库路径
    public String dbpath;

    public JSONObject singleMetaJsonObject;

    // 操作数据表的类对象
    public Query query;

    public MetaJson(String database){
        this.database = database;
        this.dbpath = MetaData.DATABASES_DIR + "/" + this.database;
        this.path = this.dbpath+"/"+MetaData.SINGLE_META_FILENAME;

        try{
            this.singleMetaJsonObject = new JSONObject(FileUtils.readFileToString(new File(this.path),"UTF-8"));
            this.query = new Query();
            for(String tableName : this.singleMetaJsonObject.keySet()){
                Storage storage = new Storage(
                        Storage.CONSTRUCT_FROM_EXISTED_DB,
                        tableName,
                        getTablePath(tableName),
                        getAttributesType(tableName),
                        getAttributesName(tableName),
                        getAttributesPKType(tableName),
                        getAttributesPKName(tableName),
                        getAttributesOffset(tableName),
                        getAttributesNotNull(tableName)
                );
                this.query.initLoadTable(tableName,storage);
            }

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

    public Boolean hasTable(String tableName){
        return singleMetaJsonObject.has(tableName);
    }

    public String createTable(String tableName, JSONObject tableInfo ){
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
            query.downLoadTable(tableName);
            writeBack();
            return String.format("have dropped table: %s",tableName);
        }
        return String.format("cannot drop a non-exist table: %s",tableName);
    }

    public String showTableInfo(String tableName){
        if(hasTable(tableName)){
            Vector<String> attr = getAttributesName(tableName);
            Vector<Integer> type = getAttributesType(tableName);
            StringBuilder result = new StringBuilder("------------\n" + tableName + "\n------------\n");
            assert attr.size() == type.size();
            for(int i = 0; i < attr.size(); i++){
                result.append(attr.get(i));
                result.append(" ");
                result.append(Type.TYPE_REVERSE_MAP.get(type.get(i)));
                result.append("\n");
            }
            result.append("------------");
            return result.toString();
        }
        return "could not show info of a non-exist table";

    }

    public String getTableList(){
        return database + " : "+singleMetaJsonObject.keySet().toString();
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
            return result;
        }
        throw new CustomerException("table not exist");
    }

}