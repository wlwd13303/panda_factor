import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Card,
  Table,
  Button,
  Space,
  Tag,
  message,
  Modal,
  Input,
  Tooltip,
  Select,
} from 'antd'
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  SearchOutlined,
} from '@ant-design/icons'
import dayjs from 'dayjs'
import {
  getUserFactorList,
  deleteFactor,
  runFactor,
  type UserFactor,
} from '@/api/userFactor'
import './index.css'

const { Search } = Input
const { Option } = Select

const FactorList = () => {
  const navigate = useNavigate()
  const [data, setData] = useState<UserFactor[]>([])
  const [loading, setLoading] = useState(false)
  const [total, setTotal] = useState(0)
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [sortField, setSortField] = useState('created_at')
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc')
  const [searchKeyword, setSearchKeyword] = useState('')

  // 默认用户ID（实际应该从登录状态获取）
  const userId = 'default_user'

  useEffect(() => {
    loadFactorList()
  }, [currentPage, pageSize, sortField, sortOrder])

  const loadFactorList = async () => {
    setLoading(true)
    try {
      const response = await getUserFactorList({
        user_id: userId,
        page: currentPage,
        page_size: pageSize,
        sort_field: sortField,
        sort_order: sortOrder,
      })
      
      setData(response.factors || [])
      setTotal(response.total || 0)
    } catch (error: any) {
      console.error('Failed to load factor list:', error)
      
      // 显示更友好的错误信息
      if (error.response?.status === 500) {
        message.error('因子服务器错误，请确认服务已启动并连接到数据库')
      } else if (error.response?.status === 404) {
        message.error('因子服务不可用，请检查服务器配置')
      } else if (error.code === 'ERR_NETWORK') {
        message.error('无法连接到因子服务器，请确认服务已在8765端口启动')
      } else {
        message.error(error.message || '加载因子列表失败')
      }
      
      // 清空数据，避免显示旧数据
      setData([])
      setTotal(0)
    } finally {
      setLoading(false)
    }
  }

  const handleCreate = () => {
    navigate('/factor-editor/new')
  }

  const handleEdit = (record: UserFactor) => {
    navigate(`/factor-editor/${record.factor_id}`)
  }

  const handleDelete = (record: UserFactor) => {
    Modal.confirm({
      title: '确认删除',
      content: `确定要删除因子 "${record.name}" 吗？`,
      okText: '确定',
      cancelText: '取消',
      onOk: async () => {
        try {
          await deleteFactor(record.factor_id)
          message.success('删除成功')
          loadFactorList()
        } catch (error: any) {
          message.error(error.message || '删除失败')
        }
      },
    })
  }

  const handleRun = async (record: UserFactor) => {
    try {
      const response: any = await runFactor(record.factor_id)
      message.success('因子分析任务已启动')
      
      // 跳转到分析结果页面
      if (response.task_id) {
        navigate(`/factor-result/${response.task_id}`)
      }
    } catch (error: any) {
      message.error(error.message || '启动失败')
    }
  }

  const columns = [
    {
      title: '因子名称',
      dataIndex: 'name',
      key: 'name',
      width: 200,
      ellipsis: true,
    },
    {
      title: '标识符',
      dataIndex: 'factor_name',
      key: 'factor_name',
      width: 150,
      ellipsis: true,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: number) => {
        const statusMap: Record<number, { text: string; color: string }> = {
          0: { text: '未运行', color: 'default' },
          1: { text: '运行中', color: 'processing' },
          2: { text: '已完成', color: 'success' },
          3: { text: '失败', color: 'error' },
        }
        const info = statusMap[status] || statusMap[0]
        return <Tag color={info.color}>{info.text}</Tag>
      },
    },
    {
      title: 'IC',
      dataIndex: 'IC',
      key: 'IC',
      width: 100,
      sorter: true,
      render: (val: number) => (val ? val.toFixed(4) : '-'),
    },
    {
      title: 'IR',
      dataIndex: 'IR',
      key: 'IR',
      width: 100,
      sorter: true,
      render: (val: number) => (val ? val.toFixed(4) : '-'),
    },
    {
      title: '年化收益率',
      dataIndex: 'return_ratio',
      key: 'return_ratio',
      width: 120,
      sorter: true,
      render: (val: number) => (val ? `${(val * 100).toFixed(2)}%` : '-'),
    },
    {
      title: '夏普比',
      dataIndex: 'sharpe_ratio',
      key: 'sharpe_ratio',
      width: 100,
      sorter: true,
      render: (val: number) => (val ? val.toFixed(2) : '-'),
    },
    {
      title: '最大回撤',
      dataIndex: 'maximum_drawdown',
      key: 'maximum_drawdown',
      width: 120,
      sorter: true,
      render: (val: number) => (val ? `${(val * 100).toFixed(2)}%` : '-'),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
      sorter: true,
      render: (val: string) => (val ? dayjs(val).format('YYYY-MM-DD HH:mm:ss') : '-'),
    },
    {
      title: '操作',
      key: 'action',
      fixed: 'right' as const,
      width: 200,
      render: (_: any, record: UserFactor) => (
        <Space size="small">
          <Tooltip title="编辑">
            <Button
              type="link"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="运行分析">
            <Button
              type="link"
              icon={<PlayCircleOutlined />}
              onClick={() => handleRun(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="删除">
            <Button
              type="link"
              danger
              icon={<DeleteOutlined />}
              onClick={() => handleDelete(record)}
              size="small"
            />
          </Tooltip>
        </Space>
      ),
    },
  ]

  return (
    <div className="factor-list-page">
      <Card
        title="因子管理"
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={loadFactorList}>
              刷新
            </Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
              新建因子
            </Button>
          </Space>
        }
      >
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          {/* 搜索和筛选 */}
          <Space>
            <Search
              placeholder="搜索因子名称"
              onSearch={(value) => {
                setSearchKeyword(value)
                loadFactorList()
              }}
              style={{ width: 300 }}
              allowClear
            />
            <Select
              style={{ width: 150 }}
              placeholder="排序字段"
              value={sortField}
              onChange={setSortField}
            >
              <Option value="created_at">创建时间</Option>
              <Option value="updated_at">更新时间</Option>
              <Option value="return_ratio">收益率</Option>
              <Option value="sharpe_ratio">夏普比</Option>
              <Option value="IC">IC</Option>
              <Option value="IR">IR</Option>
            </Select>
            <Select
              style={{ width: 100 }}
              value={sortOrder}
              onChange={setSortOrder}
            >
              <Option value="desc">降序</Option>
              <Option value="asc">升序</Option>
            </Select>
          </Space>

          {/* 表格 */}
          <Table
            columns={columns}
            dataSource={data}
            rowKey="factor_id"
            loading={loading}
            scroll={{ x: 1500 }}
            locale={{
              emptyText: (
                <div style={{ padding: '40px 0' }}>
                  <p style={{ fontSize: '16px', marginBottom: '8px' }}>暂无因子数据</p>
                  <p style={{ color: '#999', fontSize: '14px' }}>
                    {total === 0 ? '点击"新建因子"开始创建您的第一个因子' : '加载失败，请检查服务器连接'}
                  </p>
                </div>
              ),
            }}
            pagination={{
              current: currentPage,
              pageSize: pageSize,
              total: total,
              showSizeChanger: true,
              showQuickJumper: true,
              showTotal: (total) => `共 ${total} 条`,
              onChange: (page, pageSize) => {
                setCurrentPage(page)
                setPageSize(pageSize)
              },
            }}
            onChange={(pagination, filters, sorter: any) => {
              if (sorter.field) {
                setSortField(sorter.field)
                setSortOrder(sorter.order === 'ascend' ? 'asc' : 'desc')
              }
            }}
          />
        </Space>
      </Card>
    </div>
  )
}

export default FactorList

