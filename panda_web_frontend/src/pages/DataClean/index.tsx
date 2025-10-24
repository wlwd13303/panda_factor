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
  Radio,
  Input,
} from 'antd'
import { PlayCircleOutlined, ReloadOutlined } from '@ant-design/icons'
import dayjs, { Dayjs } from 'dayjs'
import ProgressPanel from '@/components/ProgressPanel'
import {
  getStockProgress,
  getFactorProgress,
  getFinancialProgress,
  startStockClean,
  startFactorClean,
  startFinancialClean,
  getDataSourceConfig,
  updateDataSourceConfig,
} from '@/api/dataClean'
import type { ProgressData, DataSourceConfig } from '@/types'
import './index.css'

const { RangePicker } = DatePicker
const { TextArea } = Input

const DataClean = () => {
  const [stockForm] = Form.useForm()
  const [factorForm] = Form.useForm()
  const [financialForm] = Form.useForm()

  const [stockProgress, setStockProgress] = useState<ProgressData | null>(null)
  const [factorProgress, setFactorProgress] = useState<ProgressData | null>(null)
  const [financialProgress, setFinancialProgress] = useState<ProgressData | null>(null)
  const [stockLoading, setStockLoading] = useState(false)
  const [factorLoading, setFactorLoading] = useState(false)
  const [financialLoading, setFinancialLoading] = useState(false)
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
      const [stock, factor, financial] = await Promise.all([
        getStockProgress(),
        getFactorProgress(),
        getFinancialProgress(),
      ])
      setStockProgress(stock)
      setFactorProgress(factor)
      setFinancialProgress(financial)
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

  const handleStartFinancialClean = async () => {
    try {
      const values = await financialForm.validateFields()
      
      setFinancialLoading(true)
      
      // 构建请求参数
      const params: any = {}
      
      // 处理报告期参数
      if (values.periodMode === 'single') {
        // 单个报告期或最新报告期
        if (values.singlePeriod) {
          params.periods = values.singlePeriod
        }
        // 如果没有选择，后端会自动使用最新报告期
      } else if (values.periodMode === 'range') {
        // 报告期范围
        if (values.periodRange && values.periodRange.length === 2) {
          params.period_start = values.periodRange[0]
          params.period_end = values.periodRange[1]
        }
      }
      
      // 处理股票范围参数
      if (values.stockScope === 'specific' && values.symbols) {
        params.symbols = values.symbols.trim()
      }
      // 如果是全市场，不传symbols参数
      
      // 处理数据类型参数
      if (values.dataTypes && values.dataTypes.length > 0) {
        params.data_types = values.dataTypes.join(',')
      }
      
      await startFinancialClean(params)
      
      message.success('财务数据清洗任务已启动')
      setTimeout(loadProgress, 1000)
    } catch (error: any) {
      message.error(error.message || '启动失败')
    } finally {
      setFinancialLoading(false)
    }
  }

  // 生成报告期选项（最近8个季度）
  const generatePeriodOptions = () => {
    const options = []
    const now = dayjs()
    const currentYear = now.year()
    const currentMonth = now.month() + 1
    
    // 定义季度结束月份和日期
    const quarters = [
      { month: 3, day: 31, label: 'Q1' },
      { month: 6, day: 30, label: 'Q2' },
      { month: 9, day: 30, label: 'Q3' },
      { month: 12, day: 31, label: 'Q4' },
    ]
    
    let count = 0
    for (let year = currentYear; year >= currentYear - 2 && count < 8; year--) {
      for (let i = quarters.length - 1; i >= 0 && count < 8; i--) {
        const q = quarters[i]
        const periodDate = dayjs(`${year}-${q.month}-${q.day}`)
        
        // 只显示已经过去的季度
        if (periodDate.isBefore(now)) {
          const value = `${year}${q.month.toString().padStart(2, '0')}${q.day.toString().padStart(2, '0')}`
          const label = `${year}${q.label} (${value})`
          options.push({ label, value })
          count++
        }
      }
    }
    
    return options
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

          {/* 财务数据清洗 */}
          <Col xs={24} lg={12}>
            <Card
              title="财务数据清洗"
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
                form={financialForm}
                layout="vertical"
                initialValues={{
                  periodMode: 'single',
                  singlePeriod: undefined,
                  stockScope: 'all',
                  dataTypes: ['income', 'balance', 'cashflow', 'indicator'],
                }}
              >
                <Form.Item
                  label="报告期选择"
                  name="periodMode"
                  rules={[{ required: true }]}
                >
                  <Radio.Group>
                    <Radio value="single">单个报告期</Radio>
                    <Radio value="range">报告期范围</Radio>
                  </Radio.Group>
                </Form.Item>

                <Form.Item noStyle shouldUpdate={(prev, curr) => prev.periodMode !== curr.periodMode}>
                  {({ getFieldValue }) => {
                    const mode = getFieldValue('periodMode')
                    if (mode === 'single') {
                      return (
                        <Form.Item
                          label="报告期"
                          name="singlePeriod"
                          tooltip="不选择则默认使用最新报告期"
                        >
                          <Select
                            placeholder="选择报告期（默认最新）"
                            allowClear
                            options={generatePeriodOptions()}
                          />
                        </Form.Item>
                      )
                    } else {
                      return (
                        <Form.Item
                          label="报告期范围"
                          name="periodRange"
                          rules={[{ required: true, message: '请选择报告期范围' }]}
                        >
                          <Select
                            mode="multiple"
                            placeholder="选择起始和结束报告期"
                            maxTagCount={2}
                            options={generatePeriodOptions()}
                          />
                        </Form.Item>
                      )
                    }
                  }}
                </Form.Item>

                <Form.Item
                  label="股票范围"
                  name="stockScope"
                  rules={[{ required: true }]}
                >
                  <Radio.Group>
                    <Radio value="all">全市场</Radio>
                    <Radio value="specific">指定股票</Radio>
                  </Radio.Group>
                </Form.Item>

                <Form.Item noStyle shouldUpdate={(prev, curr) => prev.stockScope !== curr.stockScope}>
                  {({ getFieldValue }) => {
                    const scope = getFieldValue('stockScope')
                    if (scope === 'specific') {
                      return (
                        <Form.Item
                          label="股票代码"
                          name="symbols"
                          rules={[{ required: true, message: '请输入股票代码' }]}
                          tooltip="多个股票用逗号分隔，如：000001.SZ,600519.SH。超过50只建议使用全市场模式"
                        >
                          <TextArea
                            placeholder="000001.SZ,600519.SH"
                            rows={2}
                          />
                        </Form.Item>
                      )
                    }
                    return null
                  }}
                </Form.Item>

                <Form.Item
                  label="数据类型"
                  name="dataTypes"
                  rules={[{ required: true, message: '请选择至少一种数据类型' }]}
                >
                  <Select
                    mode="multiple"
                    placeholder="选择数据类型"
                    options={[
                      { label: '利润表', value: 'income' },
                      { label: '资产负债表', value: 'balance' },
                      { label: '现金流量表', value: 'cashflow' },
                      { label: '财务指标', value: 'indicator' },
                    ]}
                  />
                </Form.Item>

                <Form.Item>
                  <Button
                    type="primary"
                    icon={<PlayCircleOutlined />}
                    onClick={handleStartFinancialClean}
                    loading={financialLoading}
                    block
                  >
                    启动清洗
                  </Button>
                </Form.Item>
              </Form>

              <Divider />

              <ProgressPanel
                title="清洗进度"
                data={financialProgress}
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

