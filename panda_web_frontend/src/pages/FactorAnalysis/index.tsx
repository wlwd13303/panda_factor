import { useState } from 'react'
import {
  Card,
  Form,
  Input,
  DatePicker,
  Button,
  Select,
  InputNumber,
  Space,
  message,
  Divider,
  Table,
  Tag,
} from 'antd'
import { PlayCircleOutlined, DownloadOutlined } from '@ant-design/icons'
import dayjs, { Dayjs } from 'dayjs'
import { startFactorAnalysis, getFactorAnalysisResult } from '@/api/factor'
import type { FactorAnalysisParams, FactorAnalysisResult } from '@/types'
import './index.css'

const { RangePicker } = DatePicker

const FactorAnalysis = () => {
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)
  const [results, setResults] = useState<FactorAnalysisResult[]>([])

  const handleStartAnalysis = async () => {
    try {
      const values = await form.validateFields()
      const [start, end] = values.dateRange as [Dayjs, Dayjs]
      
      const params: FactorAnalysisParams = {
        factor_name: values.factor_name,
        start_date: start.format('YYYY-MM-DD'),
        end_date: end.format('YYYY-MM-DD'),
        universe: values.universe,
        industry: values.industry,
        quantile: values.quantile,
      }
      
      setLoading(true)
      const response = await startFactorAnalysis(params)
      
      message.success('因子分析任务已启动')
      
      // 模拟获取结果（实际应该根据task_id轮询）
      setTimeout(async () => {
        try {
          // 这里应该使用返回的task_id
          const result = await getFactorAnalysisResult('mock_task_id')
          setResults([result, ...results])
          message.success('分析完成')
        } catch (error) {
          console.error('Failed to get result:', error)
        } finally {
          setLoading(false)
        }
      }, 3000)
    } catch (error: any) {
      message.error(error.message || '启动失败')
      setLoading(false)
    }
  }

  const columns = [
    {
      title: '因子名称',
      dataIndex: 'factor_name',
      key: 'factor_name',
    },
    {
      title: 'IC均值',
      dataIndex: 'ic_mean',
      key: 'ic_mean',
      render: (val: number) => val?.toFixed(4) || '-',
    },
    {
      title: 'IC标准差',
      dataIndex: 'ic_std',
      key: 'ic_std',
      render: (val: number) => val?.toFixed(4) || '-',
    },
    {
      title: 'IR',
      dataIndex: 'ir',
      key: 'ir',
      render: (val: number) => val?.toFixed(4) || '-',
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const colorMap: Record<string, string> = {
          completed: 'success',
          running: 'processing',
          error: 'error',
        }
        return <Tag color={colorMap[status] || 'default'}>{status}</Tag>
      },
    },
    {
      title: '操作',
      key: 'action',
      render: () => (
        <Button
          type="link"
          icon={<DownloadOutlined />}
          size="small"
        >
          导出
        </Button>
      ),
    },
  ]

  return (
    <div className="factor-analysis-page">
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Card title="因子分析配置">
          <Form
            form={form}
            layout="vertical"
            initialValues={{
              dateRange: [dayjs().subtract(1, 'year'), dayjs()],
              quantile: 5,
              universe: 'all',
            }}
          >
            <Form.Item
              label="因子名称"
              name="factor_name"
              rules={[{ required: true, message: '请输入因子名称' }]}
            >
              <Input placeholder="例如: momentum_20d" />
            </Form.Item>

            <Form.Item
              label="回测日期"
              name="dateRange"
              rules={[{ required: true, message: '请选择日期范围' }]}
            >
              <RangePicker style={{ width: '100%' }} />
            </Form.Item>

            <Form.Item label="股票池" name="universe">
              <Select>
                <Select.Option value="all">全市场</Select.Option>
                <Select.Option value="hs300">沪深300</Select.Option>
                <Select.Option value="zz500">中证500</Select.Option>
                <Select.Option value="zz1000">中证1000</Select.Option>
              </Select>
            </Form.Item>

            <Form.Item label="行业" name="industry">
              <Select allowClear placeholder="不限">
                <Select.Option value="finance">金融</Select.Option>
                <Select.Option value="tech">科技</Select.Option>
                <Select.Option value="consumer">消费</Select.Option>
                <Select.Option value="healthcare">医药</Select.Option>
              </Select>
            </Form.Item>

            <Form.Item label="分组数" name="quantile">
              <InputNumber min={3} max={10} style={{ width: '100%' }} />
            </Form.Item>

            <Form.Item>
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={handleStartAnalysis}
                loading={loading}
                block
                size="large"
              >
                开始分析
              </Button>
            </Form.Item>
          </Form>
        </Card>

        <Card title="分析结果">
          <Table
            columns={columns}
            dataSource={results}
            rowKey="factor_name"
            pagination={{ pageSize: 10 }}
            locale={{ emptyText: '暂无分析结果' }}
          />
        </Card>
      </Space>
    </div>
  )
}

export default FactorAnalysis

