/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import defHttp from '@/lib/api'

const Apis = {
  GET: ({ metalake, catalog, schema }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/tables`,
  GET_DETAIL: ({ metalake, catalog, schema, table }) =>
    `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/tables/${table}`
}

export const getTablesApi = params => {
  return defHttp.request({
    url: `${Apis.GET(params)}`,
    method: 'get'
  })
}

export const getTableDetailsApi = ({ metalake, catalog, schema, table }) => {
  return defHttp.request({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, table })}`,
    method: 'get'
  })
}
