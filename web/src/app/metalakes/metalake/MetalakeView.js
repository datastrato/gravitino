/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect } from 'react'

import { Box } from '@mui/material'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import MetalakePageLeftBar from './MetalakePageLeftBar'
import RightContent from './rightContent/RightContent'

import {
  fetchCatalogs,
  fetchSchemas,
  fetchTables,
  fetchFilesets,
  fetchTopics,
  getMetalakeDetails,
  getCatalogDetails,
  getSchemaDetails,
  getTableDetails,
  getFilesetDetails,
  getTopicDetails,
  setSelectedNodes
} from '@/lib/store/metalakes'

const MetalakeView = () => {
  const dispatch = useAppDispatch()
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length
  const store = useAppSelector(state => state.metalakes)

  useEffect(() => {
    const routeParams = {
      metalake: searchParams.get('metalake'),
      catalog: searchParams.get('catalog'),
      type: searchParams.get('type'),
      schema: searchParams.get('schema'),
      table: searchParams.get('table'),
      fileset: searchParams.get('fileset'),
      topic: searchParams.get('topic')
    }
    if ([...searchParams.keys()].length) {
      const { metalake, catalog, type, schema, table, fileset, topic } = routeParams

      if (paramsSize === 1 && metalake) {
        dispatch(fetchCatalogs({ init: true, page: 'metalakes', metalake }))
        dispatch(getMetalakeDetails({ metalake }))
      }

      if (paramsSize === 3 && catalog) {
        if (!store.catalogs.length) {
          dispatch(fetchCatalogs({ metalake }))
        }
        dispatch(fetchSchemas({ init: true, page: 'catalogs', metalake, catalog, type }))
        dispatch(getCatalogDetails({ metalake, catalog, type }))
      }

      if (paramsSize === 4 && catalog && type && schema) {
        if (!store.catalogs.length) {
          dispatch(fetchCatalogs({ metalake }))
          dispatch(fetchSchemas({ metalake, catalog, type }))
        }
        switch (type) {
          case 'relational':
            dispatch(fetchTables({ init: true, page: 'schemas', metalake, catalog, schema }))
            break
          case 'fileset':
            dispatch(fetchFilesets({ init: true, page: 'schemas', metalake, catalog, schema }))
            break
          case 'messaging':
            dispatch(fetchTopics({ init: true, page: 'schemas', metalake, catalog, schema }))
            break
          default:
            break
        }
        dispatch(getSchemaDetails({ metalake, catalog, schema }))
      }

      if (paramsSize === 5 && catalog && schema) {
        if (!store.catalogs.length) {
          dispatch(fetchCatalogs({ metalake }))
          dispatch(fetchSchemas({ metalake, catalog, type }))
        }
        if (table) {
          dispatch(getTableDetails({ init: true, metalake, catalog, schema, table }))
        }
        if (fileset) {
          dispatch(getFilesetDetails({ init: true, metalake, catalog, schema, fileset }))
        }
        if (topic) {
          dispatch(getTopicDetails({ init: true, metalake, catalog, schema, topic }))
        }
      }
    }

    dispatch(
      setSelectedNodes(
        routeParams.catalog
          ? [
              `{{${routeParams.metalake}}}{{${routeParams.catalog}}}{{${routeParams.type}}}${
                routeParams.schema ? `{{${routeParams.schema}}}` : ''
              }${routeParams.table ? `{{${routeParams.table}}}` : ''}${
                routeParams.fileset ? `{{${routeParams.fileset}}}` : ''
              }${routeParams.topic ? `{{${routeParams.topic}}}` : ''}`
            ]
          : []
      )
    )

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams])

  return (
    <Box className={'metalake-template'} style={{ height: 'calc(100vh - 11rem)' }}>
      <Box className='app-metalake twc-w-full twc-h-full twc-flex twc-rounded-lg twc-overflow-auto twc-relative shadow-md'>
        <Box className={`twc-bg-customs-white`} sx={{ borderRight: theme => `1px solid ${theme.palette.divider}` }}>
          <Box className={`twc-w-[340px] twc-h-full round-tl-md round-bl-md twc-overflow-hidden`}>
            <Box className={'twc-px-5 twc-py-3 twc-flex twc-items-center'}></Box>
            <MetalakePageLeftBar />
          </Box>
        </Box>
        <RightContent />
      </Box>
    </Box>
  )
}

export default MetalakeView
