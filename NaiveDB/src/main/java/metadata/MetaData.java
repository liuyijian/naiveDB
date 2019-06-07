package metadata;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;
import util.CustomerException;
import storage.Type;


public class MetaData  {

    public static final String DATABASES_DIR = "databases";
    public static final String WHOLE_META_FILENAME = "auth.json";
    public static final String SINGLE_META_FILENAME = "meta.json";
    public static final String DB_NAME = "test.db";
    public static final int WRITEBACK_INTERVAL =  5;

    public JSONObject wholeMetaJsonObject;
    public JSONArray usersList;
    public JSONArray databasesList;
    public JSONObject passwordMap;
    public JSONObject authMap;

    public String currentUser;
    public String currentDatabase;

    public MetaJson metaJson;


    public MetaData() throws IOException {
        // 若文件不存在，则抛出异常
        wholeMetaJsonObject = new JSONObject(FileUtils.readFileToString(
                new File(DATABASES_DIR+"/"+WHOLE_META_FILENAME), "UTF-8"));
        usersList = wholeMetaJsonObject.getJSONArray("users_list");
        databasesList = wholeMetaJsonObject.getJSONArray("databases_list");
        passwordMap = wholeMetaJsonObject.getJSONObject("password_map");
        authMap = wholeMetaJsonObject.getJSONObject("auth_map");
        currentUser = null;
        currentDatabase = null;
        metaJson = null;
    }

    public String getCurrentUser(){
        return currentUser;
    }

    public String getCurrentDatabase(){
        return currentDatabase;
    }

    public int getWritebackInterval(){
        return WRITEBACK_INTERVAL;
    }

    public boolean existsUser(String username) {
        return usersList.toList().contains(username);
    }

    public boolean existsDatabase(String database) {
        return databasesList.toList().contains(database);
    }

    public String login(String username, String password) {
        if(existsUser(username)){
            if(password.equals(passwordMap.getString(username))){
                currentUser = username;
                return String.format("login as %s", username);
            }
            return String.format("wrong password for %s", username);
        }
        return String.format("%s is not a registered user", username);
    }
    
    public String logout() {
    	try {
			metaJson.save();
	        currentUser = null;
	        currentDatabase = null;
	        metaJson = null;
		} catch (IOException e) {
		}
    	return "Bye";
    }

    public boolean checkAuth(String database) {
        // 检查操作是否具有权限
        if(existsDatabase(database)){
             return authMap.getJSONArray(database).toList().contains(currentUser);
        }
        return false;
    }

    public boolean checkAdmin(){
        return "admin".equals(currentUser);
    }

    public String switchDatabase(String database) {
        // 选择数据库，用于配合命令 USE DATABASE <string:db_name>;
        if (existsDatabase(database)) {
            if (checkAuth(database)){
                currentDatabase = database;
                // 新建一个metaJson对象，以后插表建表就用它了
                metaJson = new MetaJson(currentDatabase);
                return String.format("%s is selected", database);
            }
            return String.format("You don't have permission on %s", database);
        }
        return "cannot switch to a not-exist database";
    }

    public String showDatabases() {
        // 展示所有数据库，用于配合命令 SHOW DATABASES;
        return databasesList.toString();
//        String str = "";
//        for(int i = 0; i < databasesList.length(); i++) {
//            str += databasesList.getString(i);
//            str += '\n';
//        }
//        return str.substring(0, str.length()-1);
    }

    public String showDatabaseTables(String database){
        // 展示某个数据库的所有表，用于配合命令 SHOW DATABASE <String:db_name>;
        if (existsDatabase(database)){
            if (checkAuth(database)){
                MetaJson metaJson = new MetaJson(database);
                return metaJson.getTableList();
            }
            return String.format("You don't have permission on %s", database);
        }
        return String.format("cannot show tables for a not-exist database: %s",database);
    }

    public String createDatabase(String database) {
        if (!existsDatabase(database)){
            if (checkAdmin()) {
                // 添加到databases_list
                databasesList.put(database);
                // 添加到auth-map
                String[] tmp = {"admin"};
                JSONArray tmp1 = new JSONArray();
                tmp1.put("admin");
                authMap.put(database, tmp1);
                // 创建目录
                try {
                    FileUtils.forceMkdir(new File(DATABASES_DIR + "/" + database));
                } catch (IOException e) {
                    return "fail to create a database";
                }
                // 创建meta.json文件
                try {
                    FileUtils.writeStringToFile(new File(DATABASES_DIR+"/"+database+"/"+SINGLE_META_FILENAME), "{}","UTF-8");
                } catch (IOException e) {
                    return "fail to create " + SINGLE_META_FILENAME + " for database:" + database;
                }
                writeBack();
                return String.format("database: %s has been created",database);
            }
            return String.format("You don't have permission to create database:%s", database);
        }
        return "cannot create an already-exist database";
    }

    public String dropDatabase(String database) {
        if (existsDatabase(database)){
            if (checkAdmin()){
                // 从databasesList中删除database
                int position = -1;
                for(int i = 0; i < databasesList.length(); i++){
                    if (database.equals(databasesList.getString(i))){
                        position = i;
                        break;
                    }
                }
                try {
                    databasesList.remove(position);
                } catch (Exception e){
                    System.out.println("No such element to remove");
                }

                // 从auth-map中删除database
                authMap.remove(database);

                // 删除对应数据库目录
                FileUtils.deleteQuietly(new File(DATABASES_DIR+"/"+database));

                // 如果删了正在使用的数据库，则要更新currentDatabase，和 metaJson 对象
                if (database.equals(currentDatabase)){
                    currentDatabase = null;
                    metaJson = null;
                }
                writeBack();

                return String.format("Database: %s has been deleted",database);
            }
            return String.format("You don't have permission to drop database:%s", database);
        }
        return "cannot delete a not-exist database";
    }

    // 此处的创建和删除表函数仅针对元数据的维护
    public String createTable(String tableName, JSONObject tableInfo){
        // currentDatabase 被设置时能确保它一定合法
        if (existsDatabase(currentDatabase)){
            // 添加meta.json中此表的信息
            return metaJson.createTable(tableName, tableInfo);
        }
        return "no database has been selected";
    }

    public String dropTable(String tableName) {
        if (existsDatabase(currentDatabase)){
            // 删去meta.json中此表的信息
            return metaJson.dropTable(tableName);
        }
        return "no database has been selected";
    }

    public void writeBack(){
        try{
            //重新组装wholeMetaJsonObject对象
            wholeMetaJsonObject.put("users_list", usersList);
            wholeMetaJsonObject.put("databases_list", databasesList);
            wholeMetaJsonObject.put("auth_map", authMap);
            FileUtils.writeStringToFile(new File(DATABASES_DIR+"/"+WHOLE_META_FILENAME), wholeMetaJsonObject.toString(), "UTF-8");
        }
        catch (Exception e) {
//            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        MetaData metaData;
        try {
            metaData = new MetaData();
        } catch (Exception e){
            return;
        }

//        System.out.println(metaData.login("user_unknown", ""));
//        System.out.println(metaData.login("admin","wrong"));
        System.out.println(metaData.login("admin","123456"));
//        System.out.println(metaData.showDatabases());

//        System.out.println(metaData.showDatabaseTables("not_exist_database"));
//        System.out.println(metaData.switchDatabase("not_exist_database"));

        System.out.println(metaData.switchDatabase("database1"));
        System.out.println(metaData.metaJson.getAttributesType("person"));
        System.out.println(metaData.metaJson.getAttributesName("person"));
        System.out.println(metaData.metaJson.getAttributesPKType("person"));
        System.out.println(metaData.metaJson.getAttributesPKName("person"));
        System.out.println(metaData.metaJson.getAttributesNotNull("person"));
        System.out.println(metaData.metaJson.getAttributesOffset("person"));

//        System.out.println(metaData.createDatabase("database_new"));
//        System.out.println(metaData.showDatabases());
//        System.out.println(metaData.switchDatabase("database_new"));
//        System.out.println(metaData.dropDatabase("database_new"));
//        // 此句应该失败，因为此时没有数据库被选中
//        System.out.println(metaData.createTable("table1",new JSONObject("{}")));
//        System.out.println(metaData.switchDatabase("database1"));
//        System.out.println(metaData.showDatabaseTables("database1"));
//        // 此句应该失败，因为表已经存在
//        System.out.println(metaData.createTable("table1",new JSONObject("{}")));
//
//        System.out.println(metaData.createTable("table_new",new JSONObject("{}")));
//        System.out.println(metaData.showDatabaseTables("database1"));
//        System.out.println(metaData.dropTable("table_new"));
//        System.out.println(metaData.showDatabaseTables("database1"));
    }

}

