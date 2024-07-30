/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useState, forwardRef, useEffect } from 'react'
import { updateMetalake } from '@/lib/store/metalakes'

import {
  Box,
  Button,
  Dialog,
  Typography,
  DialogContent,
  DialogActions,
  IconButton,
  Fade,
  Grid,
  TextField,
  FormControl,
  FormHelperText,
} from '@mui/material'

import Icon from '@/components/Icon'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { createMetalake } from '@/lib/store/metalakes'

import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { groupBy } from 'lodash-es'
import { genUpdates } from '@/lib/utils'
import { nameRegex, nameRegexDesc, keyRegex } from '@/lib/utils/regex'

const defaultValues = {
  name: '',
  comment: '',
}

const schema = yup.object().shape({
  name: yup.string().required().matches(nameRegex, nameRegexDesc),
})

const Transition = forwardRef(function Transition(props, ref) {
  return <Fade ref={ref} {...props} />
})

const CreateMetalakeDialog = props => {
  const { open, setOpen, data = {}, type = 'create' } = props

  const dispatch = useAppDispatch()

  const [innerProps, setInnerProps] = useState([])
  const [cacheData, setCacheData] = useState()

  const typeText = type === 'create' ? 'Create' : 'Edit'

  const {
    control,
    reset,
    setValue,
    handleSubmit,
    formState: { errors },
  } = useForm({
    defaultValues,
    mode: 'onChange',
    resolver: yupResolver(schema),
  })

  const handleFormChange = (index, event) => {
    let data = [...innerProps]
    data[index][event.target.name] = event.target.value

    if (event.target.name === 'key') {
      const invalidKey = !keyRegex.test(event.target.value)
      data[index].invalid = invalidKey
    }

    const nonEmptyKeys = data.filter(item => item.key.trim() !== '')
    const grouped = groupBy(nonEmptyKeys, 'key')
    const duplicateKeys = Object.keys(grouped).some(key => grouped[key].length > 1)

    if (duplicateKeys) {
      data[index].hasDuplicateKey = duplicateKeys
    } else {
      data.forEach(it => (it.hasDuplicateKey = false))
    }

    setInnerProps(data)
  }

  const addFields = () => {
    const duplicateKeys = innerProps
      .filter(item => item.key.trim() !== '')
      .some(
        (item, index, filteredItems) =>
          filteredItems.findIndex(otherItem => otherItem !== item && otherItem.key.trim() === item.key.trim()) !== -1
      )

    if (duplicateKeys) {
      return
    }

    let newField = { key: '', value: '' }

    setInnerProps([...innerProps, newField])
  }

  const removeFields = index => {
    let data = [...innerProps]
    data.splice(index, 1)
    setInnerProps(data)
  }

  const handleClose = () => {
    reset({ name: '', comment: '' })
    setInnerProps([])
    setOpen(false)
  }

  const onSubmit = data => {
    const duplicateKeys = innerProps
      .filter(item => item.key.trim() !== '')
      .some(
        (item, index, filteredItems) =>
          filteredItems.findIndex(otherItem => otherItem !== item && otherItem.key.trim() === item.key.trim()) !== -1
      )

    const invalidKeys = innerProps.some(i => i.invalid)

    if (duplicateKeys || invalidKeys) {
      return
    }

    const properties = innerProps
      .filter(i => i.key.trim() !== '')
      .reduce((acc, item) => {
        acc[item.key] = item.value

        return acc
      }, {})

    const metalakeData = {
      ...data,
      properties,
    }

    if (type === 'create') {
      dispatch(createMetalake({ ...metalakeData })).then(res => {
        if (!res.payload?.err) {
          handleClose()
        }
      })
    } else {
      const reqData = { updates: genUpdates(cacheData, metalakeData) }

      if (reqData.updates.length !== 0) {
        dispatch(updateMetalake({ name: cacheData.name, data: reqData })).then(res => {
          if (!res.payload?.err) {
            handleClose()
          }
        })
      }
    }
  }

  useEffect(() => {
    if (open && JSON.stringify(data) !== '{}') {
      setCacheData(data)
      const { properties = {} } = data

      const propsArr = Object.keys(properties).map(item => {
        return {
          key: item,
          value: properties[item],
        }
      })

      setInnerProps(propsArr)

      setValue('name', data.name)
      setValue('comment', data.comment)
    }
  }, [open, data, setValue])

  return (
    <Dialog fullWidth maxWidth='sm' scroll='body' TransitionComponent={Transition} open={open} onClose={handleClose}>
      <form onSubmit={handleSubmit(onSubmit)}>
        <DialogContent
          className={'twc-relative twc-pb-8'}
          sx={{
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`],
          }}
        >
          <IconButton
            className={'twc-absolute twc-right-[1rem] twc-top-[1rem]'}
            size='small'
            onClick={() => handleClose()}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box className={'twc-mb-8 twc-text-center'}>
            <Typography className={'twc-mb-3'} variant='h5' data-refer='handle-metalake-title'>
              {typeText} Metalake
            </Typography>
          </Box>

          <Grid container spacing={6}>
            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='name'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      data-refer='metalake-name-field'
                      label='Name'
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.name)}
                    />
                  )}
                />
                {errors.name && (
                  <FormHelperText className={'twc-text-error-main'} data-refer='metalake-name-field-error-text'>
                    {errors.name.message}
                  </FormHelperText>
                )}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='comment'
                  control={control}
                  rules={{ required: false }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      data-refer='metalake-comment-field'
                      label='Comment'
                      multiline
                      rows={2}
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.comment)}
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12} id='properties' data-refer='metalake-props-layout'>
              <Typography className={'twc-mb-2'} variant='body2'>
                Properties
              </Typography>
              {innerProps.map((item, index) => {
                return (
                  <Grid item xs={12} key={index} className={'[&+&]:twc-mt-2'}>
                    <FormControl fullWidth>
                      <Box
                        className={'twc-flex twc-items-center twc-justify-between'}
                        data-refer='metalake-props-wrapper'
                      >
                        <TextField
                          size='small'
                          name='key'
                          label='Key'
                          value={item.key}
                          onChange={event => handleFormChange(index, event)}
                          error={item.hasDuplicateKey}
                          data-refer={`add-props-key-${index}`}
                        />
                        <TextField
                          size='small'
                          name='value'
                          label='Value'
                          value={item.value}
                          onChange={event => handleFormChange(index, event)}
                          data-refer={`add-props-value-${index}`}
                        />
                        <Box className={'twc-min-w-[40px]'}>
                          <IconButton onClick={() => removeFields(index)}>
                            <Icon icon='mdi:minus-circle-outline' />
                          </IconButton>
                        </Box>
                      </Box>
                      {item.hasDuplicateKey && (
                        <FormHelperText className={'twc-text-error-main'}>Key already exists</FormHelperText>
                      )}
                      {item.invalid && (
                        <FormHelperText className={'twc-text-error-main'}>
                          Invalid key, matches strings starting with a letter/underscore, followed by alphanumeric
                          characters, underscores, hyphens, or dots.
                        </FormHelperText>
                      )}
                    </FormControl>
                  </Grid>
                )
              })}
            </Grid>

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addFields}
                variant='outlined'
                data-refer='add-metalake-props'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
              >
                Add Property
              </Button>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions className={' twc-px-5 md:twc-px-15 twc-pb-5 md:twc-pb-[12.5 0.25 rem]'}>
          <Button variant='contained' type='submit' data-refer='submit-handle-metalake'>
            {typeText === 'Edit' ? 'Update' : typeText}
          </Button>
          <Button
            variant='outlined'
            className={'twc-ml-1'}
            onClick={handleClose}
            type='reset'
            data-refer='cancel-handle-metalake'
          >
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export default CreateMetalakeDialog
