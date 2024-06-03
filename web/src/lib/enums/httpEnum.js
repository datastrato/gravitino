/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

/*
MIT License

Copyright (c) 2020-present, Vben

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

/**
 * Referred from src/enums/httpEnum.ts
 */
export var ContentTypeEnum;

(function (ContentTypeEnum) {
  ContentTypeEnum['JSON'] = 'application/json;charset=UTF-8';
  ContentTypeEnum['FORM_URLENCODED'] = 'application/x-www-form-urlencoded;charset=UTF-8';
  ContentTypeEnum['FORM_DATA'] = 'multipart/form-data;charset=UTF-8';
})(ContentTypeEnum || (ContentTypeEnum = {}));

export var RequestEnum;

(function (RequestEnum) {
  RequestEnum['GET'] = 'GET';
  RequestEnum['POST'] = 'POST';
  RequestEnum['PUT'] = 'PUT';
  RequestEnum['DELETE'] = 'DELETE';
})(RequestEnum || (RequestEnum = {}));

export var ResultEnum;

(function (ResultEnum) {
  ResultEnum[(ResultEnum['SUCCESS'] = 0)] = 'SUCCESS';
  ResultEnum[(ResultEnum['ERROR'] = -1)] = 'ERROR';
  ResultEnum[(ResultEnum['TIMEOUT'] = 401)] = 'TIMEOUT';
  ResultEnum['TYPE'] = 'success';
})(ResultEnum || (ResultEnum = {}));
