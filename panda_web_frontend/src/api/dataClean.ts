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

// 财务数据清洗相关接口

// 获取财务数据清洗进度
export const getFinancialProgress = () => {
  return axios.get<any, ProgressData>('/datahub/api/v1/get_financial_progress')
}

// 按报告期清洗财务数据
export const startFinancialClean = (params: {
  periods?: string              // 报告期（单个或多个，逗号分隔）
  period_start?: string         // 报告期范围开始
  period_end?: string           // 报告期范围结束
  symbols?: string              // 股票代码（逗号分隔）
  data_types?: string           // 数据类型（逗号分隔）
  use_vip?: boolean             // 是否使用VIP接口
}) => {
  return axios.post('/datahub/api/v1/clean_financial_by_periods', null, { params })
}

