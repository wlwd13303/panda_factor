import { useState, useRef, useEffect } from 'react'
import { Card, Input, Button, Space, Avatar, Spin, Empty, message } from 'antd'
import {
  SendOutlined,
  RobotOutlined,
  UserOutlined,
  DeleteOutlined,
} from '@ant-design/icons'
import { sendChatMessage } from '@/api/llm'
import './index.css'

const { TextArea } = Input

interface Message {
  role: 'user' | 'assistant'
  content: string
  timestamp: number
}

const LLMChat = () => {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const [sessionId, setSessionId] = useState<string | null>(null) // 不初始化，让后端创建
  
  const userId = 'default_user'

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  const handleSend = async () => {
    if (!input.trim() || loading) return

    const userMessage: Message = {
      role: 'user',
      content: input,
      timestamp: Date.now(),
    }

    setMessages((prev) => [...prev, userMessage])
    setInput('')
    setLoading(true)

    try {
      const response = await sendChatMessage({
        user_id: userId,
        message: input,
        session_id: sessionId || undefined, // 第一次不传session_id，让后端创建
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
              if (data === '[DONE]') {
                break
              }

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
      // 移除失败的助手消息
      setMessages((prev) => prev.slice(0, -1))
    } finally {
      setLoading(false)
    }
  }

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  const handleClear = () => {
    setMessages([])
    setSessionId(null) // 清空会话ID，下次会创建新会话
    message.success('聊天记录已清空，将创建新会话')
  }

  return (
    <div className="llm-chat-page">
      <Card
        title={
          <Space>
            <RobotOutlined />
            <span>AI 助手</span>
          </Space>
        }
        extra={
          <Button icon={<DeleteOutlined />} onClick={handleClear} size="small">
            清空对话
          </Button>
        }
        style={{ height: 'calc(100vh - 150px)' }}
        bodyStyle={{ height: 'calc(100% - 57px)', display: 'flex', flexDirection: 'column' }}
      >
        {/* 消息列表 */}
        <div className="messages-container">
          {messages.length === 0 ? (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="开始与AI助手对话"
              style={{ marginTop: '100px' }}
            />
          ) : (
            <>
              {messages.map((msg, index) => (
                <div
                  key={index}
                  className={`message-item ${msg.role === 'user' ? 'user-message' : 'assistant-message'}`}
                >
                  <Avatar
                    icon={msg.role === 'user' ? <UserOutlined /> : <RobotOutlined />}
                    style={{
                      backgroundColor: msg.role === 'user' ? '#1890ff' : '#52c41a',
                    }}
                  />
                  <div className="message-content">
                    <div className="message-bubble">
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
        <div className="input-container">
          <TextArea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="输入消息... (Shift+Enter 换行，Enter 发送)"
            autoSize={{ minRows: 2, maxRows: 6 }}
            disabled={loading}
          />
          <Button
            type="primary"
            icon={<SendOutlined />}
            onClick={handleSend}
            loading={loading}
            size="large"
            style={{ marginTop: '8px' }}
            block
          >
            发送
          </Button>
        </div>
      </Card>
    </div>
  )
}

export default LLMChat

