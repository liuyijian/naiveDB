package util;

public class CustomerException extends RuntimeException {

    public String code;
    public String msg;

    public CustomerException(){
        super();
    }

    public CustomerException(String message){
        super();
        msg = message;
    }

    public CustomerException(String errorCode, String message){
        super();
        code = errorCode;
        msg = message;
    }
}
