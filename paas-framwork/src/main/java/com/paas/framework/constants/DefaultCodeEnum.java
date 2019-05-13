package com.paas.framework.constants;

public enum DefaultCodeEnum {
    //SUCCESS_CODE 成功 、SYS_EX_CODE 系统异常、BUSS_EX_CODE、业务异常
    SUCCESS_CODE("0000"), SYS_EX_CODE("9999"), BUSS_EX_CODE("9998"), PARAMETERS_EX_CODE("10003");
    private final String code;

    DefaultCodeEnum(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    ;
}
