import axios from './axios'

// 因子相关类型
export interface FactorParams {
  start_date: string
  end_date: string
  adjustment_cycle: number
  stock_pool: string
  factor_direction: boolean
  group_number: number
  include_st: boolean
  extreme_value_processing: string
}

export interface UserFactor {
  factor_id: string
  user_id: string
  name: string
  factor_name: string
  factor_type: string
  is_persistent: boolean
  cron: string
  factor_start_day: string
  code: string
  code_type: string
  status: number
  describe: string
  tags: string
  params: FactorParams
  created_at?: string
  updated_at?: string
  // 性能指标
  return_ratio?: number
  sharpe_ratio?: number
  maximum_drawdown?: number
  IC?: number
  IR?: number
}

export interface FactorListParams {
  user_id: string
  page?: number
  page_size?: number
  sort_field?: string
  sort_order?: 'asc' | 'desc'
}

export interface FactorListResponse {
  factors: UserFactor[]
  total: number
  page: number
  page_size: number
}

export interface CreateFactorRequest {
  user_id: string
  name: string
  factor_name: string
  factor_type?: string
  is_persistent?: boolean
  cron?: string
  factor_start_day: string
  code: string
  code_type?: string
  status?: number
  describe?: string
  tags?: string
  params: FactorParams
}

// 获取用户因子列表
export const getUserFactorList = (params: FactorListParams) => {
  return axios.get<any, FactorListResponse>('/panda_factor/api/v1/user_factor_list', { params })
}

// 创建因子
export const createFactor = (data: CreateFactorRequest) => {
  return axios.post('/panda_factor/api/v1/create_factor', data)
}

// 更新因子
export const updateFactor = (factor_id: string, data: CreateFactorRequest) => {
  return axios.post('/panda_factor/api/v1/update_factor', data, { params: { factor_id } })
}

// 删除因子
export const deleteFactor = (factor_id: string) => {
  return axios.get('/panda_factor/api/v1/delete_factor', { params: { factor_id } })
}

// 查询因子详情
export const queryFactor = (factor_id: string) => {
  return axios.get<any, UserFactor>('/panda_factor/api/v1/query_factor', { params: { factor_id } })
}

// 运行因子
export const runFactor = (factor_id: string) => {
  return axios.get('/panda_factor/api/v1/run_factor', { params: { factor_id } })
}

// 查询任务状态
export const queryTaskStatus = (task_id: string) => {
  return axios.get('/panda_factor/api/v1/query_task_status', { params: { task_id } })
}

// 查询任务日志
export const getTaskLogs = (task_id: string, last_log_id?: string) => {
  return axios.get('/panda_factor/api/v1/task_logs', { 
    params: { task_id, last_log_id } 
  })
}

// 查询因子分析数据
export const queryFactorAnalysisData = (task_id: string) => {
  return axios.get('/panda_factor/api/v1/query_factor_analysis_data', { params: { task_id } })
}

// 查询超额收益图表
export const queryFactorExcessChart = (task_id: string) => {
  return axios.get('/panda_factor/api/v1/query_factor_excess_chart', { params: { task_id } })
}

// 查询收益曲线
export const queryReturnChart = (task_id: string) => {
  return axios.get('/panda_factor/api/v1/query_return_chart', { params: { task_id } })
}

// 查询IC序列
export const queryICSequenceChart = (task_id: string) => {
  return axios.get('/panda_factor/api/v1/query_ic_sequence_chart', { params: { task_id } })
}

// 查询分组收益分析
export const queryGroupReturnAnalysis = (task_id: string) => {
  return axios.get('/panda_factor/api/v1/query_group_return_analysis', { params: { task_id } })
}

