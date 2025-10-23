import axios from './axios'
import type { FactorAnalysisParams, FactorAnalysisResult } from '@/types'

// 获取因子分析进度
export const getFactorAnalysisProgress = (task_id: string) => {
  return axios.get(`/panda_factor/api/v1/factor/progress/${task_id}`)
}

// 启动因子分析
export const startFactorAnalysis = (params: FactorAnalysisParams) => {
  return axios.post('/panda_factor/api/v1/factor/analyze', params)
}

// 获取因子分析结果
export const getFactorAnalysisResult = (task_id: string) => {
  return axios.get<any, FactorAnalysisResult>(`/panda_factor/api/v1/factor/result/${task_id}`)
}

// 获取可用因子列表
export const getAvailableFactors = () => {
  return axios.get('/panda_factor/api/v1/factor/list')
}

