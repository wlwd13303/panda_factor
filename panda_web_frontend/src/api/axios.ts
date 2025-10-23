import axios, { AxiosError, AxiosResponse } from 'axios'
import { message } from 'antd'
import type { ApiResponse } from '@/types'

// 创建axios实例
const instance = axios.create({
  timeout: 60000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// 请求拦截器
instance.interceptors.request.use(
  (config) => {
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// 响应拦截器
instance.interceptors.response.use(
  (response: AxiosResponse<ApiResponse>) => {
    const { data } = response
    
    // 如果是进度查询等特殊接口，直接返回数据
    if (response.config.url?.includes('get_progress')) {
      return response.data
    }
    
    // 统一处理业务错误
    if (data.code !== undefined && data.code !== 0 && data.code !== 200) {
      message.error(data.message || '请求失败')
      return Promise.reject(new Error(data.message || '请求失败'))
    }
    
    return response.data
  },
  (error: AxiosError) => {
    // 处理HTTP错误
    if (error.response) {
      const status = error.response.status
      switch (status) {
        case 400:
          message.error('请求参数错误')
          break
        case 401:
          message.error('未授权，请登录')
          break
        case 403:
          message.error('拒绝访问')
          break
        case 404:
          message.error('请求地址不存在')
          break
        case 500:
          message.error('服务器内部错误')
          break
        default:
          message.error(`请求失败: ${status}`)
      }
    } else if (error.request) {
      message.error('网络错误，请检查网络连接')
    } else {
      message.error('请求失败')
    }
    
    return Promise.reject(error)
  }
)

export default instance

