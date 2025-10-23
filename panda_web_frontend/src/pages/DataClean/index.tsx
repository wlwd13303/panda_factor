import { useState, useEffect } from 'react'
import {
  Card,
  Form,
  DatePicker,
  Button,
  Space,
  Row,
  Col,
  Select,
  message,
  Divider,
  Tag,
} from 'antd'
import { PlayCircleOutlined, ReloadOutlined } from '@ant-design/icons'
import dayjs, { Dayjs } from 'dayjs'
import ProgressPanel from '@/components/ProgressPanel'
import {
  getStockProgress,
  getFactorProgress,
  startStockClean,
  startFactorClean,
  getDataSourceConfig,
  updateDataSourceConfig,
} from '@/api/dataClean'
import type { ProgressData, DataSourceConfig } from '@/types'
import './index.css'

const { RangePicker } = DatePicker

const DataClean = () => {
  const [stockForm] = Form.useForm()
  const [factorForm] = Form.useForm()

  const [stockProgress, setStockProgress] = useState<ProgressData | null>(null)
  const [factorProgress, setFactorProgress] = useState<ProgressData | null>(null)
  const [stockLoading, setStockLoading] = useState(false)
  const [factorLoading, setFactorLoading] = useState(false)
  const [config, setConfig] = useState<DataSourceConfig | null>(null)

  // 轮询进度
  useEffect(() => {
    loadProgress()
    loadConfig()
    
    const interval = setInterval(() => {
      loadProgress()
    }, 2000)

    return () => clearInterval(interval)
  }, [])

  const loadProgress = async () => {
    try {
      const [stock, factor] = await Promise.all([
        getStockProgress(),
        getFactorProgress(),
      ])
      setStockProgress(stock)
      setFactorProgress(factor)
    } catch (error) {
      console.error('Failed to load progress:', error)
    }
  }

  const loadConfig = async () => {
    try {
      const response = await getDataSourceConfig()
      // 后端返回的数据结构: {DATAHUBSOURCE: "tushare"}
      const dataSource = response?.DATAHUBSOURCE || response || 'tushare'
      const configData = {
        stock_data_source: dataSource,
        factor_data_source: dataSource,
      }
      setConfig(configData)
    } catch (error) {
      console.error('Failed to load config:', error)
      // 设置默认值
      const defaultConfig = {
        stock_data_source: 'tushare',
        factor_data_source: 'tushare',
      }
      setConfig(defaultConfig)
    }
  }

  const handleStartStockClean = async () => {
    try {
      const values = await stockForm.validateFields()
      const [start, end] = values.dateRange as [Dayjs, Dayjs]
      
      setStockLoading(true)
      await startStockClean({
        start_date: start.format('YYYY-MM-DD'),
        end_date: end.format('YYYY-MM-DD'),
        data_source: values.data_source,
      })
      
      message.success('股票数据清洗任务已启动')
      setTimeout(loadProgress, 1000)
    } catch (error: any) {
      message.error(error.message || '启动失败')
    } finally {
      setStockLoading(false)
    }
  }

  const handleStartFactorClean = async () => {
    try {
      const values = await factorForm.validateFields()
      const [start, end] = values.dateRange as [Dayjs, Dayjs]
      
      setFactorLoading(true)
      await startFactorClean({
        start_date: start.format('YYYY-MM-DD'),
        end_date: end.format('YYYY-MM-DD'),
        data_source: values.data_source,
      })
      
      message.success('因子数据清洗任务已启动')
      setTimeout(loadProgress, 1000)
    } catch (error: any) {
      message.error(error.message || '启动失败')
    } finally {
      setFactorLoading(false)
    }
  }

  const handleSaveConfig = async () => {
    message.warning('数据源配置需要在服务器端修改配置文件')
    // TODO: 实现完整的配置更新功能，需要更多参数（token、数据库配置等）
    // try {
    //   const values = await configForm.validateFields()
    //   const requestData = {
    //     data_source: values.stock_data_source,
    //     // 需要更多必填参数...
    //   }
    //   await updateDataSourceConfig(requestData)
    //   setConfig(values)
    //   message.success('配置已保存')
    // } catch (error: any) {
    //   message.error(error.message || '保存失败')
    // }
  }

  return (
    <div className="data-clean-page">
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* 数据源配置 */}
        <Card title="数据源配置" size="small">
          <Space>
            <span>当前数据源：</span>
            <Tag color="blue" style={{ fontSize: '14px' }}>
              {config?.stock_data_source?.toUpperCase() || 'TUSHARE'}
            </Tag>
            <span style={{ color: '#999', fontSize: '12px' }}>
              （需要修改请在服务器端配置文件中设置）
            </span>
          </Space>
        </Card>

        <Row gutter={[16, 16]}>
          {/* 股票数据清洗 */}
          <Col xs={24} lg={12}>
            <Card
              title="股票行情数据清洗"
              extra={
                <Button
                  icon={<ReloadOutlined />}
                  onClick={loadProgress}
                  size="small"
                >
                  刷新
                </Button>
              }
            >
              <Form
                form={stockForm}
                layout="vertical"
                initialValues={{
                  dateRange: [dayjs().subtract(7, 'day'), dayjs()],
                  data_source: config?.stock_data_source || 'tushare',
                }}
              >
                <Form.Item
                  label="日期范围"
                  name="dateRange"
                  rules={[{ required: true, message: '请选择日期范围' }]}
                >
                  <RangePicker style={{ width: '100%' }} />
                </Form.Item>

                <Form.Item
                  label="数据源"
                  name="data_source"
                  rules={[{ required: true }]}
                >
                  <Select>
                    <Select.Option value="tushare">Tushare</Select.Option>
                    <Select.Option value="xtquant">XtQuant</Select.Option>
                    <Select.Option value="ricequant">RiceQuant</Select.Option>
                  </Select>
                </Form.Item>

                <Form.Item>
                  <Button
                    type="primary"
                    icon={<PlayCircleOutlined />}
                    onClick={handleStartStockClean}
                    loading={stockLoading}
                    block
                  >
                    启动清洗
                  </Button>
                </Form.Item>
              </Form>

              <Divider />

              <ProgressPanel
                title="清洗进度"
                data={stockProgress}
                loading={false}
              />
            </Card>
          </Col>

          {/* 因子数据清洗 */}
          <Col xs={24} lg={12}>
            <Card
              title="因子数据清洗"
              extra={
                <Button
                  icon={<ReloadOutlined />}
                  onClick={loadProgress}
                  size="small"
                >
                  刷新
                </Button>
              }
            >
              <Form
                form={factorForm}
                layout="vertical"
                initialValues={{
                  dateRange: [dayjs().subtract(7, 'day'), dayjs()],
                  data_source: config?.factor_data_source || 'tushare',
                }}
              >
                <Form.Item
                  label="日期范围"
                  name="dateRange"
                  rules={[{ required: true, message: '请选择日期范围' }]}
                >
                  <RangePicker style={{ width: '100%' }} />
                </Form.Item>

                <Form.Item
                  label="数据源"
                  name="data_source"
                  rules={[{ required: true }]}
                >
                  <Select>
                    <Select.Option value="tushare">Tushare</Select.Option>
                    <Select.Option value="xtquant">XtQuant</Select.Option>
                    <Select.Option value="ricequant">RiceQuant</Select.Option>
                  </Select>
                </Form.Item>

                <Form.Item>
                  <Button
                    type="primary"
                    icon={<PlayCircleOutlined />}
                    onClick={handleStartFactorClean}
                    loading={factorLoading}
                    block
                  >
                    启动清洗
                  </Button>
                </Form.Item>
              </Form>

              <Divider />

              <ProgressPanel
                title="清洗进度"
                data={factorProgress}
                loading={false}
              />
            </Card>
          </Col>
        </Row>
      </Space>
    </div>
  )
}

export default DataClean

