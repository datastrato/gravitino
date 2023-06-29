package com.datastrato.graviton.client;

import com.datastrato.graviton.dto.responses.BaseResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;

public abstract class ErrorHandler implements Consumer<BaseResponse> {

  public abstract BaseResponse parseResponse(int code, String json, ObjectMapper mapper);
}
