/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useState, useEffect, Fragment } from 'react'

import Link from 'next/link'

import { Box, Typography, Portal, Tooltip, IconButton } from '@mui/material'
import { DataGrid, GridToolbar } from '@mui/x-data-grid'
import {
  VisibilityOutlined as ViewIcon,
  EditOutlined as EditIcon,
  DeleteOutlined as DeleteIcon
} from '@mui/icons-material'

import { formatToDateTime } from '@/lib/utils/date'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { fetchMetalakes, setFilteredMetalakes, deleteMetalake, resetTree } from '@/lib/store/metalakes'
import ConfirmDeleteDialog from '@/components/ConfirmDeleteDialog'

const TableBody = props => {
  const { value, setOpenDialog, setDialogData, setDialogType, setDrawerData, setOpenDrawer } = props

  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)
  const [paginationModel, setPaginationModel] = useState({ page: 0, pageSize: 10 })

  const [openConfirmDelete, setOpenConfirmDelete] = useState(false)
  const [confirmCacheData, setConfirmCacheData] = useState(null)

  const handleDeleteMetalake = name => () => {
    setOpenConfirmDelete(true)
    setConfirmCacheData(name)
  }

  const handleConfirmDeleteSubmit = () => {
    if (confirmCacheData) {
      dispatch(deleteMetalake(confirmCacheData))
      setOpenConfirmDelete(false)
    }
  }

  const handleCloseConfirm = () => {
    setOpenConfirmDelete(false)
    setConfirmCacheData(null)
  }

  const handleShowEditDialog = data => () => {
    setDialogType('update')
    setOpenDialog(true)
    setDialogData(data)
  }

  const handleShowDetails = row => () => {
    setDrawerData(row)
    setOpenDrawer(true)
  }

  const handleClickLink = () => {
    dispatch(resetTree())
  }

  useEffect(() => {
    dispatch(fetchMetalakes())
  }, [dispatch])

  useEffect(() => {
    const filteredData = store.metalakes
      .filter(i => i.name.toLowerCase().includes(value.toLowerCase()))
      .sort((a, b) => {
        if (a.name.toLowerCase() === value.toLowerCase()) return -1
        if (b.name.toLowerCase() === value.toLowerCase()) return 1

        return 0
      })

    dispatch(setFilteredMetalakes(filteredData))
  }, [dispatch, store.metalakes, value])

  /** @type {import('@mui/x-data-grid').GridColDef[]} */
  const columns = [
    {
      flex: 0.2,
      minWidth: 230,
      disableColumnMenu: true,
      filterable: true,
      type: 'string',
      field: 'name',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Tooltip title={row.comment} placement='right'>
              <Typography
                noWrap
                component={Link}
                href={`/metalakes?metalake=${name}`}
                onClick={() => handleClickLink()}
                sx={{
                  fontWeight: 500,
                  color: 'primary.main',
                  textDecoration: 'none',
                  maxWidth: 240,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  '&:hover': { color: 'primary.main', textDecoration: 'underline' }
                }}
                data-refer={`metalake-link-${name}`}
              >
                {name}
              </Typography>
            </Tooltip>
          </Box>
        )
      }
    },
    {
      flex: 0.15,
      minWidth: 150,
      disableColumnMenu: true,
      type: 'string',
      field: 'createdBy',
      valueGetter: params => `${params.row.audit?.creator}`,
      headerName: 'Created by',
      renderCell: ({ row }) => {
        return (
          <Typography noWrap sx={{ color: 'text.secondary' }}>
            {row.audit?.creator}
          </Typography>
        )
      }
    },
    {
      flex: 0.15,
      minWidth: 150,
      disableColumnMenu: true,
      type: 'dateTime',
      field: 'createdAt',
      valueGetter: params => new Date(params.row.audit?.createTime),
      headerName: 'Created at',
      renderCell: ({ row }) => {
        return (
          <Typography title={row.audit?.createTime} noWrap sx={{ color: 'text.secondary' }}>
            {formatToDateTime(row.audit?.createTime)}
          </Typography>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 90,
      type: 'actions',
      headerName: 'Actions',
      field: 'actions',
      renderCell: ({ id, row }) => (
        <>
          <IconButton
            title='Details'
            size='small'
            sx={{ color: theme => theme.palette.text.secondary }}
            onClick={handleShowDetails(row)}
            data-refer={`view-metalake-${row.name}`}
          >
            <ViewIcon viewBox='0 0 24 22' />
          </IconButton>

          <IconButton
            title='Edit'
            size='small'
            sx={{ color: theme => theme.palette.text.secondary }}
            onClick={handleShowEditDialog(row)}
            data-refer={`edit-metalake-${row.name}`}
          >
            <EditIcon />
          </IconButton>

          <IconButton
            title='Delete'
            size='small'
            sx={{ color: theme => theme.palette.error.light }}
            onClick={handleDeleteMetalake(row.name)}
            data-refer={`delete-metalake-${row.name}`}
          >
            <DeleteIcon />
          </IconButton>
        </>
      )
    }
  ]

  function TableToolbar(props) {
    return (
      <>
        <Fragment>
          <Portal container={() => document.getElementById('filter-panel')}>
            <Box className={`twc-flex twc-w-full twc-justify-between`}>
              <GridToolbar {...props} />
            </Box>
          </Portal>
        </Fragment>
      </>
    )
  }

  return (
    <>
      <DataGrid
        disableColumnSelector
        disableDensitySelector
        slots={{ toolbar: TableToolbar }}
        slotProps={{
          toolbar: {
            printOptions: { disableToolbarButton: true },
            csvOptions: { disableToolbarButton: true }
          }
        }}
        sx={{
          '& .MuiDataGrid-virtualScroller': {
            minHeight: 36
          },
          maxHeight: 'calc(100vh - 23.2rem)'
        }}
        data-refer='metalake-table-grid'
        getRowId={row => row?.name}
        rows={store.filteredMetalakes}
        columns={columns}
        disableRowSelectionOnClick
        onCellClick={(params, event) => event.stopPropagation()}
        onRowClick={(params, event) => event.stopPropagation()}
        pageSizeOptions={[10, 25, 50]}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
      />
      <ConfirmDeleteDialog
        open={openConfirmDelete}
        setOpen={setOpenConfirmDelete}
        confirmCacheData={confirmCacheData}
        handleClose={handleCloseConfirm}
        handleConfirmDeleteSubmit={handleConfirmDeleteSubmit}
      />
    </>
  )
}

export default TableBody
