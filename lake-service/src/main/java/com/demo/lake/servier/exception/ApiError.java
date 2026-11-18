package com.demo.lake.servier.exception;

/** 统一错误响应体：status + 一句话原因 + 可选的下一步提示 */
public record ApiError(int status, String error, String message, String hint) {
}
