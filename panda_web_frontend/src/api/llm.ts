import axios from './axios'

export interface ChatRequest {
  user_id: string
  message: string
  session_id?: string
}

export interface ChatSession {
  session_id: string
  user_id: string
  title: string
  created_at: string
  updated_at: string
  message_count: number
}

export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
  timestamp: string
}

// 发送聊天消息（流式）
export const sendChatMessage = (data: ChatRequest) => {
  return fetch('/llm/chat', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
  })
}

// 获取用户会话列表
export const getUserSessions = (user_id: string, limit: number = 10) => {
  return axios.get<any, { sessions: ChatSession[] }>('/llm/chat/sessions', {
    params: { user_id, limit }
  })
}

