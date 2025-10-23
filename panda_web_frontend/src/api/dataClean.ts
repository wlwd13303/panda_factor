import axios from './axios'
import type { ProgressData, DataSourceConfig } from '@/types'

// 获取股票数据清洗进度
export const getStockProgress = () => {
  return axios.get<any, ProgressData>('/datahub/api/v1/get_progress_stock_final')
}

// 获取因子数据清洗进度
export const getFactorProgress = () => {
  return axios.get<any, ProgressData>('/datahub/api/v1/get_progress_factor_final')
}

// 启动股票数据清洗
export const startStockClean = (params: {
  start_date: string
  end_date: string
  data_source?: string
}) => {
  return axios.post('/datahub/api/v1/upsert_stockmarket', params)
}

// 启动因子数据清洗
export const startFactorClean = (params: {
  start_date: string
  end_date: string
  data_source?: string
}) => {
  return axios.post('/datahub/api/v1/upsert_factor', params)
}

// 获取数据源配置
export const getDataSourceConfig = () => {
  return axios.get<any, any>('/datahub/api/v1/get_datahub_resource')
}

// 更新数据源配置
export const updateDataSourceConfig = (config: any) => {
  return axios.post('/datahub/api/v1/config_redefine_data_source', config)
}

