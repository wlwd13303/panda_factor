// 进度数据类型
export interface ProgressData {
  progress_percent: number
  status: 'idle' | 'running' | 'completed' | 'error'
  current_task: string
  processed_count: number
  total_count: number
  start_time?: string
  estimated_completion?: string
  current_date?: string
  error_message?: string
  data_source?: string
  batch_info?: string
  trading_days_processed?: number
  trading_days_total?: number
  stock_progress_percent?: number
  stock_phase?: string
  stock_processed?: number
  stock_total?: number
  db_write_count?: number
  last_message?: string
}

// API响应类型
export interface ApiResponse<T = any> {
  code: number
  message: string
  data: T
}

// 数据源配置
export interface DataSourceConfig {
  stock_data_source: string
  factor_data_source: string
}

// 因子分析参数
export interface FactorAnalysisParams {
  factor_name: string
  start_date: string
  end_date: string
  universe?: string
  industry?: string
  quantile?: number
}

// 因子分析结果
export interface FactorAnalysisResult {
  factor_name: string
  ic_mean: number
  ic_std: number
  ir: number
  returns: number[]
  dates: string[]
  status: string
}

// 用户因子参数
export interface UserFactorParams {
  start_date: string
  end_date: string
  adjustment_cycle: number
  stock_pool: string
  factor_direction: boolean
  group_number: number
  include_st: boolean
  extreme_value_processing: string
}

// 用户因子
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
  params: UserFactorParams
  created_at?: string
  updated_at?: string
  return_ratio?: number
  sharpe_ratio?: number
  maximum_drawdown?: number
  IC?: number
  IR?: number
}
