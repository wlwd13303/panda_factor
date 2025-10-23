import { useEffect } from 'react'
import { Card, Progress, Space, Tag, Typography } from 'antd'
import { CheckCircleOutlined, CloseCircleOutlined, LoadingOutlined } from '@ant-design/icons'
import dayjs from 'dayjs'
import type { ProgressData } from '@/types'
import './index.css'

const { Text } = Typography

interface ProgressPanelProps {
  title: string
  data: ProgressData | null
  loading: boolean
}

const ProgressPanel = ({ title, data, loading }: ProgressPanelProps) => {
  if (!data && !loading) return null

  const getStatusTag = () => {
    if (loading || !data) {
      return <Tag icon={<LoadingOutlined />} color="processing">加载中</Tag>
    }
    
    switch (data.status) {
      case 'running':
        return <Tag icon={<LoadingOutlined />} color="processing">运行中</Tag>
      case 'completed':
        return <Tag icon={<CheckCircleOutlined />} color="success">已完成</Tag>
      case 'error':
        return <Tag icon={<CloseCircleOutlined />} color="error">错误</Tag>
      default:
        return <Tag color="default">空闲</Tag>
    }
  }

  const formatTime = (timeStr?: string) => {
    if (!timeStr) return '-'
    return dayjs(timeStr).format('HH:mm:ss')
  }

  return (
    <Card 
      title={
        <Space>
          <span>{title}</span>
          {getStatusTag()}
        </Space>
      }
      className="progress-panel"
      size="small"
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {/* 主进度条 */}
        <div>
          <div style={{ marginBottom: 8 }}>
            <Text strong>总进度</Text>
            <Text type="secondary" style={{ float: 'right' }}>
              {data?.progress_percent || 0}%
            </Text>
          </div>
          <Progress 
            percent={data?.progress_percent || 0} 
            status={data?.status === 'error' ? 'exception' : 'active'}
            strokeColor={{
              '0%': '#108ee9',
              '100%': '#87d068',
            }}
          />
        </div>

        {/* 当前任务 */}
        {data?.current_task && (
          <div>
            <Text type="secondary">当前任务：</Text>
            <Text>{data.current_task}</Text>
          </div>
        )}

        {/* 详细信息 */}
        {data && (
          <Space direction="vertical" size="small" style={{ width: '100%' }}>
            {data.batch_info && (
              <Text type="secondary" style={{ fontSize: '12px' }}>{data.batch_info}</Text>
            )}
            
            {data.trading_days_total > 0 && (
              <div>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  交易日：{data.trading_days_processed || 0}/{data.trading_days_total}
                </Text>
              </div>
            )}

            {data.stock_total > 0 && (
              <div>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  股票：{data.stock_processed || 0}/{data.stock_total}
                </Text>
                {data.stock_progress_percent > 0 && (
                  <Progress 
                    percent={data.stock_progress_percent} 
                    size="small" 
                    style={{ marginTop: 4 }}
                  />
                )}
              </div>
            )}

            {data.start_time && (
              <Text type="secondary" style={{ fontSize: '12px' }}>
                开始时间：{formatTime(data.start_time)}
              </Text>
            )}

            {data.estimated_completion && data.status === 'running' && (
              <Text type="secondary" style={{ fontSize: '12px' }}>
                预计完成：{formatTime(data.estimated_completion)}
              </Text>
            )}

            {data.error_message && (
              <Text type="danger" style={{ fontSize: '12px' }}>
                错误：{data.error_message}
              </Text>
            )}

            {data.last_message && (
              <Text type="secondary" style={{ fontSize: '12px' }}>
                {data.last_message}
              </Text>
            )}
          </Space>
        )}
      </Space>
    </Card>
  )
}

export default ProgressPanel

