import { useState, useRef, useEffect } from 'react'
import {
  Row,
  Col,
  Card,
  Form,
  Input,
  DatePicker,
  Button,
  Select,
  InputNumber,
  Space,
  message,
  Table,
  Tag,
  Avatar,
  Spin,
  Empty,
  Divider,
} from 'antd'
import {
  PlayCircleOutlined,
  DownloadOutlined,
  SendOutlined,
  RobotOutlined,
  UserOutlined,
  DeleteOutlined,
} from '@ant-design/icons'
import dayjs, { Dayjs } from 'dayjs'
import { startFactorAnalysis, getFactorAnalysisResult } from '@/api/factor'
import { sendChatMessage } from '@/api/llm'
import type { FactorAnalysisParams, FactorAnalysisResult } from '@/types'
import './index.css'

const { RangePicker } = DatePicker
const { TextArea } = Input
const { Option } = Select

interface Message {
  role: 'user' | 'assistant'
  content: string
  timestamp: number
}

const FactorAnalysisWithAI = () => {
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)
  const [results, setResults] = useState<FactorAnalysisResult[]>([])
  
  // AI助手状态
  const [messages, setMessages] = useState<Message[]>([])
  const [aiInput, setAiInput] = useState('')
  const [aiLoading, setAiLoading] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const [sessionId, setSessionId] = useState<string | null>(null)
  
  const userId = 'default_user'

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

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
      
      // 模拟获取结果
      setTimeout(async () => {
        try {
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

  const handleSendMessage = async () => {
    if (!aiInput.trim() || aiLoading) return

    const userMessage: Message = {
      role: 'user',
      content: aiInput,
      timestamp: Date.now(),
    }

    setMessages((prev) => [...prev, userMessage])
    setAiInput('')
    setAiLoading(true)

    try {
      const response = await sendChatMessage({
        user_id: userId,
        message: aiInput,
        session_id: sessionId || undefined,
      })

      if (!response.ok) {
        throw new Error('请求失败')
      }

      const reader = response.body?.getReader()
      const decoder = new TextDecoder()

      let assistantContent = ''
      const assistantMessage: Message = {
        role: 'assistant',
        content: '',
        timestamp: Date.now(),
      }

      setMessages((prev) => [...prev, assistantMessage])

      if (reader) {
        while (true) {
          const { done, value } = await reader.read()
          if (done) break

          const chunk = decoder.decode(value)
          const lines = chunk.split('\n')

          for (const line of lines) {
            if (line.startsWith('data: ')) {
              const data = line.slice(6).trim()
              if (data === '[DONE]') break

              try {
                const parsed = JSON.parse(data)
                if (parsed.content) {
                  assistantContent += parsed.content
                  setMessages((prev) => {
                    const newMessages = [...prev]
                    newMessages[newMessages.length - 1].content = assistantContent
                    return newMessages
                  })
                } else if (parsed.error) {
                  message.error(parsed.error)
                  break
                }
              } catch (e) {
                console.error('解析错误:', e)
              }
            }
          }
        }
      }
    } catch (error: any) {
      message.error('发送消息失败')
      console.error(error)
      setMessages((prev) => prev.slice(0, -1))
    } finally {
      setAiLoading(false)
    }
  }

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSendMessage()
    }
  }

  const handleClearChat = () => {
    setMessages([])
    setSessionId(null)
    message.success('聊天记录已清空')
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
      render: (val: number) => (val ? val.toFixed(4) : '-'),
    },
    {
      title: 'IC标准差',
      dataIndex: 'ic_std',
      key: 'ic_std',
      render: (val: number) => (val ? val.toFixed(4) : '-'),
    },
    {
      title: 'IR',
      dataIndex: 'ir',
      key: 'ir',
      render: (val: number) => (val ? val.toFixed(4) : '-'),
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
        <Button type="link" icon={<DownloadOutlined />} size="small">
          导出
        </Button>
      ),
    },
  ]

  return (
    <div className="factor-analysis-with-ai-page">
      <Row gutter={[16, 16]}>
        {/* 左侧：因子分析 */}
        <Col xs={24} lg={14}>
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
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

                <Row gutter={16}>
                  <Col span={12}>
                    <Form.Item label="股票池" name="universe">
                      <Select>
                        <Option value="all">全市场</Option>
                        <Option value="hs300">沪深300</Option>
                        <Option value="zz500">中证500</Option>
                        <Option value="zz1000">中证1000</Option>
                      </Select>
                    </Form.Item>
                  </Col>
                  <Col span={12}>
                    <Form.Item label="分组数" name="quantile">
                      <InputNumber min={3} max={10} style={{ width: '100%' }} />
                    </Form.Item>
                  </Col>
                </Row>

                <Form.Item label="行业" name="industry">
                  <Select allowClear placeholder="不限">
                    <Option value="finance">金融</Option>
                    <Option value="tech">科技</Option>
                    <Option value="consumer">消费</Option>
                    <Option value="healthcare">医药</Option>
                  </Select>
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
                pagination={{ pageSize: 5 }}
                size="small"
                locale={{ emptyText: '暂无分析结果' }}
              />
            </Card>
          </Space>
        </Col>

        {/* 右侧：AI助手 */}
        <Col xs={24} lg={10}>
          <Card
            title={
              <Space>
                <RobotOutlined />
                <span>AI 助手</span>
              </Space>
            }
            extra={
              <Button
                icon={<DeleteOutlined />}
                onClick={handleClearChat}
                size="small"
              >
                清空
              </Button>
            }
            style={{ height: '100%' }}
            bodyStyle={{ height: 'calc(100vh - 240px)', display: 'flex', flexDirection: 'column' }}
          >
            {/* 消息列表 */}
            <div className="ai-messages-container">
              {messages.length === 0 ? (
                <Empty
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  description="向AI助手提问因子分析相关问题"
                  style={{ marginTop: '60px' }}
                />
              ) : (
                <>
                  {messages.map((msg, index) => (
                    <div
                      key={index}
                      className={`ai-message-item ${
                        msg.role === 'user' ? 'user-message' : 'assistant-message'
                      }`}
                    >
                      <Avatar
                        icon={msg.role === 'user' ? <UserOutlined /> : <RobotOutlined />}
                        style={{
                          backgroundColor: msg.role === 'user' ? '#1890ff' : '#52c41a',
                        }}
                        size="small"
                      />
                      <div className="ai-message-content">
                        <div className="ai-message-bubble">
                          {msg.content || <Spin size="small" />}
                        </div>
                      </div>
                    </div>
                  ))}
                  <div ref={messagesEndRef} />
                </>
              )}
            </div>

            {/* 输入框 */}
            <div className="ai-input-container">
              <Divider style={{ margin: '8px 0' }} />
              <TextArea
                value={aiInput}
                onChange={(e) => setAiInput(e.target.value)}
                onKeyPress={handleKeyPress}
                placeholder="输入消息... (Shift+Enter 换行，Enter 发送)"
                autoSize={{ minRows: 2, maxRows: 4 }}
                disabled={aiLoading}
              />
              <Button
                type="primary"
                icon={<SendOutlined />}
                onClick={handleSendMessage}
                loading={aiLoading}
                style={{ marginTop: '8px' }}
                block
              >
                发送
              </Button>
            </div>
          </Card>
        </Col>
      </Row>
    </div>
  )
}

export default FactorAnalysisWithAI

