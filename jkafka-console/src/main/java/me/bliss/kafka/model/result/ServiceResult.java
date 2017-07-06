package me.bliss.kafka.model.result;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.result, v 0.1 3/3/15
 *          Exp $
 */
public class ServiceResult<T> {

    public ServiceResult(){

    }

    public ServiceResult(boolean success){
        this.success = success;
    }

    private boolean success;

    private T result;

    private String errorMsg;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

}
